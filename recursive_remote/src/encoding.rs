use std::io::{BufRead, Read, Seek, Write};
use std::rc::Rc;

use anyhow::{Context, Result};
use eseb::{EncryptingWriter, SymmetricKey};
use gix::prelude::Write as GixPreludeWrite;
use gix_hash::ObjectId;
use record_reader::{Format, IoRecordReader, IoRecordWriter};
use sha2::Digest;

use crate::config::EncryptionKeys;
use crate::serialization::*;

pub fn encode_state(
    repo: &Rc<gix::Repository>,
    state: &State,
    encryption: &EncryptionKeys,
    max_object_size: usize,
) -> Result<BlobRef> {
    let state: SerializedState = state.into();
    let buf = bincode::serialize(&state).context("encode state")?;
    let (blob_ref, _size) = encode(
        repo,
        &mut buf.as_ref(),
        encryption.state_key(),
        max_object_size,
    )?;
    Ok(blob_ref)
}

pub fn encode_namespace(
    repo: &Rc<gix::Repository>,
    namespace: &Namespace,
    encryption: &EncryptionKeys,
    max_object_size: usize,
) -> Result<BlobRef> {
    let namespace: SerializedNamespace = namespace.into();
    let buf = bincode::serialize(&namespace).context("encode namespace")?;
    let (blob_ref, _size) = encode(
        repo,
        &mut buf.as_ref(),
        encryption.namespace_key(),
        max_object_size,
    )?;
    Ok(blob_ref)
}

struct SplitWriter {
    fd: std::fs::File,
    _tmp: tempfile::TempDir,
    repo: Rc<gix::Repository>,
    chunk_size: usize,
    current: Option<usize>,
    oids: Vec<ObjectId>,
    disk_buf_bytes: usize,
}

impl Write for SplitWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = match self.current.take() {
            Some(remaining) => remaining,
            None => self.chunk_size,
        };

        let n = std::cmp::min(remaining, buf.len());
        let n = self.fd.write(&buf[..n])?;
        self.disk_buf_bytes += n;
        remaining -= n;

        if remaining > 0 {
            self.current = Some(remaining);
        } else {
            self.write_one()?;
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl SplitWriter {
    fn commit(mut self) -> Result<Vec<ObjectId>> {
        self.write_one()?;
        Ok(self.oids)
    }

    fn write_one(&mut self) -> std::io::Result<()> {
        self.fd.seek(std::io::SeekFrom::Start(0))?;
        let oid = self
            .repo
            .write_stream(
                gix::object::Kind::Blob,
                self.disk_buf_bytes.try_into().expect("u64"),
                &mut self.fd,
            )
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "create blob from file"))?;
        self.oids.push(oid);
        self.fd.set_len(0)?;
        self.fd.seek(std::io::SeekFrom::Start(0))?;
        self.disk_buf_bytes = 0;
        Ok(())
    }

    fn new(repo: Rc<gix::Repository>, chunk_size: usize) -> Result<SplitWriter> {
        let _tmp = tempfile::Builder::new()
            .prefix("recursive_remote")
            .tempdir()
            .context("Unable to create temp dir.")?;
        let path = _tmp.path().join("blob");
        Ok(SplitWriter {
            repo,
            _tmp,
            chunk_size,
            current: None,
            fd: std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?,
            oids: Vec::default(),
            disk_buf_bytes: 0,
        })
    }
}

pub fn encode<R: BufRead>(
    repo: &Rc<gix::Repository>,
    reader: &mut R,
    encryption: Option<&SymmetricKey>,
    max_object_size: usize,
) -> Result<(BlobRef, usize)> {
    let mut writer = SplitWriter::new(repo.clone(), max_object_size)?;
    let (sha256, writer, bytes_copied) = match encryption {
        Some(key) => {
            let writer = IoRecordWriter::new(writer, Format::Record);
            let mut writer = EncryptingWriter::new(writer, key.clone(), /*compress=*/ true)
                .context("init encrypting writer")?;
            let (sha256, bytes_copied) =
                copy_and_hash(reader, &mut writer).context("write blob")?;
            let writer = writer.into_inner()?.into_inner();
            (sha256, writer, bytes_copied)
        }
        None => {
            let (sha256, bytes_copied) =
                copy_and_hash(reader, &mut writer).context("write blob")?;
            (sha256, writer, bytes_copied)
        }
    };
    writer.commit().context("commit blobs").map(|oids| {
        (
            BlobRef {
                resource_key: ResourceKey::Git(oids),
                sha256,
            },
            bytes_copied,
        )
    })
}

pub fn decode_state(
    repo: &Rc<gix::Repository>,
    source_ref: &StateRef,
    encryption: &EncryptionKeys,
) -> Result<State> {
    unverified::decode_unverified_state(
        repo,
        &source_ref.0.resource_key,
        encryption,
        &Some(source_ref.0.sha256),
    )
    .map(|(_, s)| s)
}

pub fn decode_namespace(
    repo: &Rc<gix::Repository>,
    source_ref: &NamespaceRef,
    encryption: &EncryptionKeys,
) -> Result<Namespace> {
    let mut buf = Vec::default();
    unverified::decode(
        repo,
        &source_ref.0.resource_key,
        &mut buf,
        encryption.namespace_key(),
        &Some(source_ref.0.sha256),
    )?;

    let namespace = bincode::deserialize::<SerializedNamespace>(&buf)
        .context("deserialize namespace.bincode")?;
    let namespace = (&namespace)
        .try_into()
        .context("deserialize namespace.bincode")?;

    Ok(namespace)
}

pub fn decode<O: Write>(
    repo: &Rc<gix::Repository>,
    source_ref: &BlobRef,
    mut writer: O,
    encryption: Option<&SymmetricKey>,
) -> Result<(BlobRef, usize)> {
    unverified::decode(
        repo,
        &source_ref.resource_key,
        &mut writer,
        encryption,
        &Some(source_ref.sha256),
    )
}

fn copy_and_hash<I: BufRead, O: Write>(
    reader: &mut I,
    writer: &mut O,
) -> Result<([u8; 32], usize)> {
    let mut bytes_copied = 0;
    let mut hasher = sha2::Sha256::default();
    loop {
        let buf = reader.fill_buf().context("copy read")?;
        bytes_copied += buf.len();
        if buf.is_empty() {
            return Ok((hasher.finalize().into(), bytes_copied));
        }
        hasher.update(buf);
        writer.write_all(buf).context("copy write")?;
        let n = buf.len();
        reader.consume(n);
    }
}

// We typically read and write our own state, but the git graph is more or less
// write only, to keep our objects referenced. The one exception is when we are
// starting from a ref. This is more or less the only case where we don't check
// the sha256.
pub mod unverified {
    use super::*;

    use eseb::DecryptingReader;

    struct SplitReader {
        oids: Vec<ObjectId>,
        repo: Rc<gix::Repository>,
        buf: Vec<u8>,
        offset: usize,
    }

    impl BufRead for SplitReader {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            let buf = &self.buf[self.offset..];
            if buf.is_empty() {
                if let Some(oid) = self.oids.pop() {
                    let mut blob = self
                        .repo
                        .find_blob(oid)
                        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "find blob"))?;
                    self.buf = blob.take_data();
                    self.offset = 0;
                }
            }

            Ok(&self.buf[self.offset..])
        }

        fn consume(&mut self, amt: usize) {
            self.offset += amt;
        }
    }

    impl Read for SplitReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let data = self.fill_buf()?;
            let n = std::cmp::min(data.len(), buf.len());
            buf[..n].copy_from_slice(&data[..n]);
            self.consume(n);
            Ok(n)
        }
    }

    impl SplitReader {
        fn new(repo: Rc<gix::Repository>, mut oids: Vec<ObjectId>) -> Result<SplitReader> {
            oids.reverse();
            Ok(SplitReader {
                oids,
                repo,
                buf: Vec::default(),
                offset: 0,
            })
        }
    }

    pub fn decode<O: Write>(
        repo: &Rc<gix::Repository>,
        resource_key: &ResourceKey,
        destination: &mut O,
        encryption: Option<&SymmetricKey>,
        want_sha256: &Option<[u8; 32]>,
    ) -> Result<(BlobRef, usize)> {
        let (sha256, size) = match (encryption, resource_key) {
            (_, ResourceKey::Annex(..)) => panic!("Annex not supported"),
            (Some(key), ResourceKey::Git(oids)) => {
                let reader = SplitReader::new(repo.clone(), oids.clone())?;
                let mut crypt_reader = DecryptingReader::new(
                    IoRecordReader::from_read(reader, Format::Record, i32::MAX as usize - 1),
                    key.clone(),
                    /*compress=*/ true,
                )
                .context("create DecryptingReader")?;
                copy_and_hash(&mut crypt_reader, destination)?
            }
            (None, ResourceKey::Git(oids)) => {
                let mut reader = SplitReader::new(repo.clone(), oids.clone())?;
                copy_and_hash(&mut reader, destination)?
            }
        };

        if let Some(want_sha256) = want_sha256 {
            if *want_sha256 != sha256 {
                anyhow::bail!(format!(
                    "Expected sha256 {:?}, got {:?}",
                    want_sha256, sha256
                ));
            }
        }

        let object_ref = BlobRef {
            resource_key: resource_key.clone(),
            sha256,
        };
        Ok((object_ref, size))
    }

    pub fn decode_unverified_state_from_tree_or_blob_oid(
        repo: &Rc<gix::Repository>,
        tree_or_blob_oid: ObjectId,
        encryption: &EncryptionKeys,
        want_sha256: &Option<[u8; 32]>,
    ) -> Result<(StateRef, State)> {
        let blobs = match repo.find_tree(tree_or_blob_oid) {
            Ok(tree) => {
                let tree = tree.decode()?;
                let mut entries = Vec::with_capacity(tree.entries.len());
                for entry in tree.entries.iter() {
                    entries.push((entry.filename.to_vec(), entry.oid.into()));
                }
                entries.sort();
                let oids: Vec<_> = entries.into_iter().map(|(_, id)| id).collect();
                oids
            }
            Err(..) => {
                vec![tree_or_blob_oid]
            }
        };

        decode_unverified_state(repo, &ResourceKey::Git(blobs), encryption, want_sha256)
    }

    pub fn decode_unverified_state(
        repo: &Rc<gix::Repository>,
        resource_key: &ResourceKey,
        encryption: &EncryptionKeys,
        want_sha256: &Option<[u8; 32]>,
    ) -> Result<(StateRef, State)> {
        let mut buf = Vec::default();
        let (state_ref, _size) = decode(
            repo,
            resource_key,
            &mut buf,
            encryption.state_key(),
            want_sha256,
        )?;

        let state =
            bincode::deserialize::<SerializedState>(&buf).context("deserialize state.bincode")?;
        let state = (&state).try_into().context("deserialize state.bincode")?;

        Ok((StateRef(state_ref), state))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;

    use super::*;
    use crate::config::{EncryptionKeys, EncryptionKeysInner};

    fn oid(hex: &str) -> ObjectId {
        ObjectId::from_hex(hex.as_bytes()).expect("valid oid")
    }

    fn init_bare_repo() -> (tempfile::TempDir, Rc<gix::Repository>) {
        let dir = tempfile::Builder::new()
            .prefix("encoding-tests")
            .tempdir()
            .expect("tempdir");
        let repo = gix::init_bare(dir.path()).expect("init bare");
        (dir, Rc::new(repo))
    }

    #[test]
    fn encode_decode_plain_roundtrip_multiple_chunks() {
        let (_dir, repo) = init_bare_repo();
        let payload = vec![0xAB; 4096];

        let mut reader = Cursor::new(payload.clone());
        let (source_ref, written) = encode(&repo, &mut reader, None, 128).expect("encode");
        assert_eq!(written, payload.len());
        assert!(source_ref.oids().len() > 1);

        let mut out = Vec::new();
        let (decoded_ref, read) = decode(&repo, &source_ref, &mut out, None).expect("decode");
        assert_eq!(read, payload.len());
        assert_eq!(payload, out);
        assert_eq!(decoded_ref.sha256, source_ref.sha256);
    }

    #[test]
    fn encode_decode_encrypted_roundtrip_multiple_chunks() {
        let (_dir, repo) = init_bare_repo();
        let payload = b"The quick brown fox jumps over the lazy dog".repeat(200);
        let key = eseb::SymmetricKey::gen_key().expect("key gen");

        let mut reader = Cursor::new(payload.clone());
        let (source_ref, written) = encode(&repo, &mut reader, Some(&key), 128).expect("encode");
        assert_eq!(written, payload.len());
        assert!(source_ref.oids().len() > 1);

        let mut out = Vec::new();
        let (_decoded_ref, read) =
            decode(&repo, &source_ref, &mut out, Some(&key)).expect("decode");
        assert_eq!(read, payload.len());
        assert_eq!(payload, out);
    }

    #[test]
    fn decode_rejects_sha256_mismatch() {
        let (_dir, repo) = init_bare_repo();
        let payload = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let mut reader = Cursor::new(payload);
        let (mut source_ref, _written) = encode(&repo, &mut reader, None, 64).expect("encode");
        source_ref.sha256 = [0; 32];

        let err = decode(&repo, &source_ref, Vec::new(), None).expect_err("must fail");
        assert!(format!("{err}").contains("Expected sha256"));
    }

    #[test]
    fn encode_state_and_namespace_roundtrip() {
        let (_dir, repo) = init_bare_repo();
        let keys = EncryptionKeys { inner: None };

        let namespace = Namespace {
            refs: HashMap::from([(
                "refs/heads/main".to_string(),
                Ref::Direct(oid("1111111111111111111111111111111111111111")),
            )]),
            pack: None,
            random_name: [2; 20],
        };
        let namespace_ref =
            NamespaceRef(encode_namespace(&repo, &namespace, &keys, 64).expect("encode namespace"));
        let namespace_roundtrip =
            decode_namespace(&repo, &namespace_ref, &keys).expect("decode namespace");
        assert!(namespace_roundtrip == namespace);

        let state = State {
            namespaces: HashMap::from([("ns".to_string(), namespace_ref)]),
            parents: Vec::new(),
        };
        let state_ref = StateRef(encode_state(&repo, &state, &keys, 64).expect("encode state"));
        let state_roundtrip = decode_state(&repo, &state_ref, &keys).expect("decode state");
        assert!(state_roundtrip == state);
    }

    #[test]
    fn encode_state_and_namespace_roundtrip_encrypted() {
        let (_dir, repo) = init_bare_repo();
        let keys = EncryptionKeys {
            inner: Some(EncryptionKeysInner {
                state_key: eseb::SymmetricKey::gen_key().expect("state key"),
                namespace_key: eseb::SymmetricKey::gen_key().expect("namespace key"),
            }),
        };

        let namespace = Namespace {
            refs: HashMap::from([(
                "refs/tags/v1".to_string(),
                Ref::Direct(oid("2222222222222222222222222222222222222222")),
            )]),
            pack: None,
            random_name: [4; 20],
        };
        let namespace_ref =
            NamespaceRef(encode_namespace(&repo, &namespace, &keys, 64).expect("encode namespace"));
        let namespace_roundtrip =
            decode_namespace(&repo, &namespace_ref, &keys).expect("decode namespace");
        assert!(namespace_roundtrip == namespace);

        let state = State {
            namespaces: HashMap::from([("encrypted".to_string(), namespace_ref)]),
            parents: Vec::new(),
        };
        let state_ref = StateRef(encode_state(&repo, &state, &keys, 64).expect("encode state"));
        let state_roundtrip = decode_state(&repo, &state_ref, &keys).expect("decode state");
        assert!(state_roundtrip == state);
    }

    #[test]
    fn decode_fails_with_missing_key_for_encrypted_data() {
        let (_dir, repo) = init_bare_repo();
        let key = eseb::SymmetricKey::gen_key().expect("key gen");
        let payload = b"encrypted payload".repeat(64);

        let mut reader = Cursor::new(payload);
        let (source_ref, _written) = encode(&repo, &mut reader, Some(&key), 128).expect("encode");

        let err = decode(&repo, &source_ref, Vec::new(), None).expect_err("must fail");
        let msg = format!("{err}");
        assert!(
            msg.contains("Expected sha256")
                || msg.contains("DecryptingReader")
                || msg.contains("decrypt")
                || msg.contains("record")
        );
    }

    #[test]
    fn decode_fails_with_wrong_key_for_encrypted_data() {
        let (_dir, repo) = init_bare_repo();
        let key = eseb::SymmetricKey::gen_key().expect("key gen");
        let wrong_key = eseb::SymmetricKey::gen_key().expect("wrong key gen");
        let payload = b"encrypted payload".repeat(64);

        let mut reader = Cursor::new(payload);
        let (source_ref, _written) = encode(&repo, &mut reader, Some(&key), 128).expect("encode");

        let err = decode(&repo, &source_ref, Vec::new(), Some(&wrong_key)).expect_err("must fail");
        let msg = format!("{err}");
        assert!(!msg.is_empty());
    }
}
