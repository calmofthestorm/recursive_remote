use std::io::{BufRead, Read, Seek, Write};
use std::path::PathBuf;
use std::rc::Rc;

use anyhow::{Context, Result};
use eseb::{EncryptingWriter, SymmetricKey};
use git2::Oid;
use record_reader::{Format, IoRecordReader, IoRecordWriter};
use sha2::Digest;

use crate::config::EncryptionKeys;
use crate::serialization::*;

pub fn encode_state(
    repo: &Rc<git2::Repository>,
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
    repo: &Rc<git2::Repository>,
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
    fd: std::io::BufWriter<std::fs::File>,
    _tmp: tempdir::TempDir,
    path: PathBuf,
    repo: Rc<git2::Repository>,
    chunk_size: usize,
    current: Option<usize>,
    oids: Vec<Oid>,
}

impl Write for SplitWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut remaining = match self.current.take() {
            Some(remaining) => remaining,
            None => self.chunk_size,
        };

        let n = std::cmp::min(remaining, buf.len());
        let n = self.fd.write(&buf[..n])?;
        remaining -= n;

        if remaining > 0 {
            self.current = Some(remaining);
        } else {
            self.fd.flush()?;
            let oid = self.repo.blob_path(&self.path).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "create blob from file")
            })?;
            self.oids.push(oid);
            self.fd.seek(std::io::SeekFrom::Start(0))?;
            self.fd.get_mut().set_len(0)?;
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.fd.flush()
    }
}

impl SplitWriter {
    fn commit(mut self) -> Result<Vec<Oid>> {
        self.fd.flush()?;
        let oid = self
            .repo
            .blob_path(&self.path)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "create blob from file"))?;
        self.oids.push(oid);
        Ok(self.oids)
    }

    fn new(repo: Rc<git2::Repository>, chunk_size: usize) -> Result<SplitWriter> {
        let _tmp =
            tempdir::TempDir::new("recursive_remote").context("Unable to create temp dir.")?;
        let path = _tmp.path().join("blob");
        Ok(SplitWriter {
            repo,
            _tmp,
            chunk_size,
            current: None,
            fd: std::io::BufWriter::new(std::fs::File::create(&path)?),
            oids: Vec::default(),
            path,
        })
    }
}

pub fn encode<R: BufRead>(
    repo: &Rc<git2::Repository>,
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
    repo: &Rc<git2::Repository>,
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
    repo: &Rc<git2::Repository>,
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
    repo: &Rc<git2::Repository>,
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
        oids: Vec<Oid>,
        repo: Rc<git2::Repository>,
        buf: Vec<u8>,
        offset: usize,
    }

    impl BufRead for SplitReader {
        fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
            let buf = &self.buf[self.offset..];
            if buf.is_empty() {
                if let Some(oid) = self.oids.pop() {
                    let blob = self
                        .repo
                        .find_blob(oid)
                        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "find blob"))?;
                    self.buf.clear();
                    // TODO: can we elide this copy?
                    self.buf.extend_from_slice(&blob.content());
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
        fn new(repo: Rc<git2::Repository>, mut oids: Vec<Oid>) -> Result<SplitReader> {
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
        repo: &Rc<git2::Repository>,
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
        repo: &Rc<git2::Repository>,
        tree_or_blob_oid: Oid,
        encryption: &EncryptionKeys,
        want_sha256: &Option<[u8; 32]>,
    ) -> Result<(StateRef, State)> {
        let blobs = match repo.find_tree(tree_or_blob_oid) {
            Ok(tree) => {
                let mut entries = Vec::with_capacity(tree.len());
                for entry in tree.iter() {
                    entries.push((entry.name_bytes().to_vec(), entry.id()));
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
        repo: &Rc<git2::Repository>,
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
