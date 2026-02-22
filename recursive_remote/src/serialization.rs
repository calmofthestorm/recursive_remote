use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::rc::Rc;

use anyhow::{Context, Result};
use gix_hash::ObjectId;
use rand::Rng;

use crate::config::EncryptionKeys;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ResourceKey {
    Git(Vec<ObjectId>),
    Annex(String),
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Ord, PartialOrd, Eq)]
pub enum SerializedResourceKey {
    Git(Vec<u8>),
    Annex(String),
}

/// A reference to a blob, such as a pack or state.bincode.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct BlobRef {
    // Where the blob may be found.
    pub resource_key: ResourceKey,

    // The sha256 hash of the contents.
    pub sha256: [u8; 32],
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Ord, PartialOrd, Eq)]
struct SerializedBlobRef {
    resource_key: SerializedResourceKey,
    sha256: [u8; 32],
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct PackRef {
    pub blob_ref: BlobRef,

    // Packs use a randomly generated name to avoid leaking the pack's hash to
    // the underlying git.
    pub random_name: [u8; 20],
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedPackRef {
    blob_ref: SerializedBlobRef,
    random_name: [u8; 20],
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct StateRef(pub BlobRef);

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Ord, PartialOrd, Eq)]
struct SerializedStateRef(SerializedBlobRef);

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct NamespaceRef(pub BlobRef);

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Ord, PartialOrd, Eq)]
struct SerializedNamespaceRef(SerializedBlobRef);

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum Ref {
    Direct(ObjectId),

    // Save the oid at the time so that we can use this to track reachability
    // later.
    Symbolic(String, Option<ObjectId>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
enum SerializedRef {
    Direct([u8; 20]),
    Symbolic(String, Option<[u8; 20]>),
}

#[derive(Clone, Eq, PartialEq)]
pub struct Namespace {
    pub refs: HashMap<String, Ref>,
    pub pack: Option<PackRef>,
    pub random_name: [u8; 20],
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SerializedNamespace {
    // The refs in the repository.
    refs: BTreeMap<String, SerializedRef>,

    // The pack.
    pack: Option<SerializedPackRef>,

    pub random_name: [u8; 20],
}

#[derive(Default, Clone, Eq, PartialEq)]
pub struct State {
    pub namespaces: HashMap<String, NamespaceRef>,
    pub parents: Vec<StateRef>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SerializedState {
    namespaces: BTreeMap<String, SerializedNamespaceRef>,

    // The parent SerializedState blobs. This mirrors Git's own history using
    // sha256.
    parents: Vec<SerializedStateRef>,
}

impl Namespace {
    pub fn new() -> Namespace {
        let random_name: [u8; 20] = rand::thread_rng().r#gen();
        Namespace {
            refs: HashMap::new(),
            pack: None,
            random_name,
        }
    }
}

impl Default for Namespace {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for StateRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for PackRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.blob_ref.fmt(f)
    }
}

impl std::fmt::Display for NamespaceRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for BlobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.resource_key.fmt(f)
    }
}

impl std::fmt::Display for ResourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceKey::Annex(key) => write!(f, "annex:{}", &key),
            ResourceKey::Git(oids) if oids.is_empty() => f.write_str("git:()"),
            ResourceKey::Git(oids) if oids.len() == 1 => write!(f, "git:{}", &oids[0]),
            ResourceKey::Git(oids) => {
                write!(f, "git:({}", &oids[0])?;
                for oid in oids[1..].iter() {
                    write!(f, ", {}", &oid)?;
                }
                f.write_str(")")
            }
        }
    }
}

impl std::fmt::Display for Ref {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ref::Direct(oid) => {
                write!(f, "direct:{}", oid)
            }
            Ref::Symbolic(name, _) => {
                write!(f, "symbolic:{}", name)
            }
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (name, namespace) in self.namespaces.iter() {
            writeln!(f, "Namespace {}: {}\n---\n", name, namespace)?;
        }
        if self.namespaces.is_empty() {
            writeln!(f, "<no namespaces>")?;
        }
        for parent in self.parents.iter() {
            writeln!(f, "Parent {}", &parent)?;
        }
        if self.parents.is_empty() {
            writeln!(f, "<no parents>")?;
        }
        Ok(())
    }
}

impl std::convert::TryFrom<&SerializedResourceKey> for ResourceKey {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedResourceKey) -> Result<ResourceKey> {
        Ok(match r {
            SerializedResourceKey::Git(s_oids) => {
                if s_oids.len() % 20 != 0 {
                    anyhow::bail!("oids are 20 bytes each");
                }
                let mut oids = Vec::with_capacity(s_oids.len() / 20);
                if !s_oids.is_empty() {
                    for oid in s_oids.chunks_exact(20) {
                        oids.push(ObjectId::from_bytes_or_panic(oid));
                    }
                }
                ResourceKey::Git(oids)
            }
            SerializedResourceKey::Annex(key) => ResourceKey::Annex(key.clone()),
        })
    }
}

impl std::convert::From<&ResourceKey> for SerializedResourceKey {
    fn from(r: &ResourceKey) -> SerializedResourceKey {
        match r {
            ResourceKey::Git(oids) => {
                let mut s_oids = Vec::with_capacity(oids.len() * 20);
                for oid in oids {
                    s_oids.extend_from_slice(oid.as_bytes());
                }
                SerializedResourceKey::Git(s_oids)
            }
            ResourceKey::Annex(key) => SerializedResourceKey::Annex(key.clone()),
        }
    }
}

impl BlobRef {
    pub fn oids(&self) -> &[ObjectId] {
        match &self.resource_key {
            ResourceKey::Git(oids) => oids,
            _ => &[],
        }
    }
}

impl std::convert::TryFrom<&SerializedBlobRef> for BlobRef {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedBlobRef) -> Result<BlobRef> {
        let resource_key = (&r.resource_key).try_into().context("resource_key")?;
        Ok(BlobRef {
            resource_key,
            sha256: r.sha256,
        })
    }
}

impl std::convert::From<&BlobRef> for SerializedBlobRef {
    fn from(r: &BlobRef) -> SerializedBlobRef {
        SerializedBlobRef {
            resource_key: (&r.resource_key).into(),
            sha256: r.sha256,
        }
    }
}

impl std::convert::TryFrom<&SerializedPackRef> for PackRef {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedPackRef) -> Result<PackRef> {
        let blob_ref = (&r.blob_ref).try_into().context("blob_ref")?;
        Ok(PackRef {
            blob_ref,
            random_name: r.random_name,
        })
    }
}

impl std::convert::From<&PackRef> for SerializedPackRef {
    fn from(r: &PackRef) -> SerializedPackRef {
        SerializedPackRef {
            blob_ref: (&r.blob_ref).into(),
            random_name: r.random_name,
        }
    }
}

impl std::convert::TryFrom<&SerializedStateRef> for StateRef {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedStateRef) -> Result<StateRef> {
        Ok(StateRef((&r.0).try_into()?))
    }
}

impl std::convert::From<&StateRef> for SerializedStateRef {
    fn from(r: &StateRef) -> SerializedStateRef {
        SerializedStateRef((&r.0).into())
    }
}

impl std::convert::TryFrom<&SerializedNamespaceRef> for NamespaceRef {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedNamespaceRef) -> Result<NamespaceRef> {
        Ok(NamespaceRef((&r.0).try_into()?))
    }
}

impl std::convert::From<&NamespaceRef> for SerializedNamespaceRef {
    fn from(r: &NamespaceRef) -> SerializedNamespaceRef {
        SerializedNamespaceRef((&r.0).into())
    }
}

impl std::convert::From<Ref> for SerializedRef {
    fn from(r: Ref) -> SerializedRef {
        match r {
            Ref::Direct(s) => SerializedRef::Direct(s.as_bytes().try_into().expect("")),
            Ref::Symbolic(s, d) => {
                SerializedRef::Symbolic(s, d.map(|d| d.as_bytes().try_into().expect("")))
            }
        }
    }
}

impl std::convert::TryFrom<SerializedRef> for Ref {
    type Error = anyhow::Error;

    fn try_from(r: SerializedRef) -> Result<Ref> {
        Ok(match r {
            SerializedRef::Direct(s) => Ref::Direct(ObjectId::from_bytes_or_panic(&s)),
            SerializedRef::Symbolic(s, d) => {
                Ref::Symbolic(s, d.map(|d| ObjectId::from_bytes_or_panic(&d)))
            }
        })
    }
}

impl Ref {
    pub fn new(user_repo: &gix::Repository, target: &str) -> Result<Ref> {
        let mut reference = user_repo.find_reference(target).context("find reference")?;
        match reference.target() {
            gix_ref::TargetRef::Object(target) => Ok(Ref::Direct(target.into())),
            gix_ref::TargetRef::Symbolic(symbolic) => {
                let symbolic = symbolic.as_bstr().to_string();

                // We will still store invalid references, but obviously we can't store their deps.
                let target = match reference.peel_to_id() {
                    Ok(target) => Some(target),
                    Err(e) => {
                        log::warn!(
                            "Unable to peel symbolic reference {} to id: {}. We will store the reference as is.",
                            target,
                            &e
                        );
                        None
                    }
                };
                Ok(Ref::Symbolic(symbolic, target.map(Into::into)))
            }
        }
    }

    pub fn shallow_equal(a: &Ref, b: &Ref) -> bool {
        match (a, b) {
            (Ref::Direct(a), Ref::Direct(b)) => a == b,
            (Ref::Symbolic(a, _), Ref::Symbolic(b, _)) => a == b,
            _ => false,
        }
    }

    pub fn oid_at_time(&self) -> Option<ObjectId> {
        match self {
            Ref::Direct(b) => Some(*b),
            Ref::Symbolic(_, b) => *b,
        }
    }

    pub fn to_git_wire_string(&self) -> String {
        match self {
            Ref::Direct(b) => b.to_string(),
            Ref::Symbolic(s, _) => s.clone(),
        }
    }
}

impl std::convert::TryFrom<&SerializedState> for State {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedState) -> Result<State> {
        let mut namespaces = HashMap::new();
        for (name, namespace_ref) in r.namespaces.iter() {
            namespaces.insert(
                name.to_string(),
                namespace_ref
                    .try_into()
                    .with_context(|| format!("Namespace {}", &name))?,
            );
        }

        let mut parents = Vec::new();
        for parent in r.parents.iter() {
            parents.push(parent.try_into().context("parent conversion")?);
        }

        Ok(State {
            namespaces,
            parents,
        })
    }
}

impl State {
    pub fn namespace(
        &self,
        namespace: &str,
        keys: &EncryptionKeys,
        tracking_repo: &Rc<gix::Repository>,
    ) -> Result<Option<Namespace>> {
        match self.namespaces.get(namespace) {
            Some(namespace_ref) => Ok(Some(
                crate::encoding::decode_namespace(tracking_repo, namespace_ref, keys)
                    .with_context(|| format!("load namespace {}", namespace))?,
            )),
            None => Ok(None),
        }
    }
}

impl std::convert::From<&State> for SerializedState {
    fn from(r: &State) -> SerializedState {
        let mut parents: Vec<_> = r.parents.iter().map(Into::into).collect();
        parents.sort();
        SerializedState {
            namespaces: r
                .namespaces
                .iter()
                .map(|(k, v)| (k.clone(), v.into()))
                .collect(),
            parents,
        }
    }
}

impl std::convert::TryFrom<&SerializedNamespace> for Namespace {
    type Error = anyhow::Error;

    fn try_from(r: &SerializedNamespace) -> Result<Namespace> {
        let mut refs = HashMap::new();
        for (k, v) in r.refs.iter() {
            refs.insert(k.clone(), (*v).clone().try_into().context("ref")?);
        }

        Ok(Namespace {
            refs,
            pack: r
                .pack
                .as_ref()
                .map(TryInto::try_into)
                .transpose()
                .context("convert pack ref")?,
            random_name: r.random_name,
        })
    }
}

impl std::convert::From<&Namespace> for SerializedNamespace {
    fn from(r: &Namespace) -> SerializedNamespace {
        SerializedNamespace {
            refs: r
                .refs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone().into()))
                .collect(),
            pack: r.pack.as_ref().map(Into::into),
            random_name: r.random_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oid(hex: &str) -> ObjectId {
        ObjectId::from_hex(hex.as_bytes()).expect("valid oid")
    }

    #[test]
    fn resource_key_git_roundtrip() {
        let key = ResourceKey::Git(vec![
            oid("1111111111111111111111111111111111111111"),
            oid("2222222222222222222222222222222222222222"),
        ]);
        let serialized: SerializedResourceKey = (&key).into();
        let decoded = ResourceKey::try_from(&serialized).expect("decode");
        assert_eq!(key, decoded);
    }

    #[test]
    fn resource_key_git_rejects_invalid_length() {
        let serialized = SerializedResourceKey::Git(vec![1, 2, 3]);
        let err = ResourceKey::try_from(&serialized).expect_err("must fail");
        assert!(format!("{err}").contains("20 bytes each"));
    }

    #[test]
    fn namespace_conversion_roundtrip() {
        let pack = PackRef {
            blob_ref: BlobRef {
                resource_key: ResourceKey::Git(vec![oid(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                )]),
                sha256: [7; 32],
            },
            random_name: [3; 20],
        };

        let mut refs = HashMap::new();
        refs.insert(
            "refs/heads/main".to_string(),
            Ref::Direct(oid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
        );
        refs.insert(
            "HEAD".to_string(),
            Ref::Symbolic(
                "refs/heads/main".to_string(),
                Some(oid("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")),
            ),
        );

        let namespace = Namespace {
            refs,
            pack: Some(pack),
            random_name: [9; 20],
        };

        let serialized: SerializedNamespace = (&namespace).into();
        let decoded = Namespace::try_from(&serialized).expect("decode namespace");
        assert!(namespace == decoded);
    }

    #[test]
    fn state_serialization_sorts_parents() {
        let mk_parent = |byte| {
            StateRef(BlobRef {
                resource_key: ResourceKey::Git(vec![oid(
                    "cccccccccccccccccccccccccccccccccccccccc",
                )]),
                sha256: [byte; 32],
            })
        };
        let low = mk_parent(1);
        let high = mk_parent(200);

        let state = State {
            namespaces: HashMap::new(),
            parents: vec![high.clone(), low.clone()],
        };

        let serialized: SerializedState = (&state).into();
        let decoded = State::try_from(&serialized).expect("decode state");

        assert_eq!(decoded.parents, vec![low, high]);
    }

    #[test]
    fn ref_helpers_behave_as_expected() {
        let direct = Ref::Direct(oid("dddddddddddddddddddddddddddddddddddddddd"));
        let symbolic = Ref::Symbolic(
            "refs/heads/main".to_string(),
            Some(oid("dddddddddddddddddddddddddddddddddddddddd")),
        );
        let symbolic_other = Ref::Symbolic("refs/heads/dev".to_string(), None);

        assert!(Ref::shallow_equal(&direct, &direct));
        assert!(Ref::shallow_equal(&symbolic, &symbolic));
        assert!(!Ref::shallow_equal(&symbolic, &symbolic_other));
        assert_eq!(
            symbolic.oid_at_time(),
            Some(oid("dddddddddddddddddddddddddddddddddddddddd"))
        );
        assert_eq!(symbolic.to_git_wire_string(), "refs/heads/main");
    }
}
