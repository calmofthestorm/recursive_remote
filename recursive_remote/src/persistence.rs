use std::collections::HashMap;
use std::convert::TryInto;
use std::rc::Rc;

use anyhow::{Context, Result};
use gix::diff::object::bstr::BStr;
use gix::object::tree::EntryKind;
use gix_hash::ObjectId;
use rand::Rng;

use crate::config::{Config, EncryptionKeys};
use crate::encoding::*;
use crate::serialization::*;
use crate::util::*;

pub fn ref_to_state_oid(
    repo: &gix::Repository,
    ref_name: &str,
) -> Result<Option<(ObjectId, ObjectId, ObjectId)>> {
    Ok(match peel_reference_to_commit(repo, ref_name)? {
        Some(commit) => {
            let commit_id = commit.id;
            let commit = commit.decode()?;
            let tree_id = commit.tree();
            let tree = repo.find_tree(tree_id)?;
            tree.find_entry("state.bincode")
                .map(|tree_entry| (commit_id, tree_id, tree_entry.oid().into()))
        }
        None => None,
    })
}

// Updates the namespace in the state.
pub fn update_state_with_push(
    config: &Config,
    tracking_repo: &Rc<gix::Repository>,
    state: &State,
    namespace: &Namespace,
    parent_state_identifier: &Option<StateRef>,
) -> Result<State> {
    let mut future = state.clone();
    future.parents = parent_state_identifier.into_iter().cloned().collect();

    let namespace_ref = NamespaceRef(
        encode_namespace(
            tracking_repo,
            namespace,
            &config.nacl_keys,
            config.max_object_size,
        )
        .context("encode pack file")?,
    );
    future
        .namespaces
        .insert(config.namespace.clone(), namespace_ref);
    Ok(future)
}

// Updates the namespace with the specified refs changes and added packs.
pub fn update_namespace_with_push(
    config: &Config,
    tracking_repo: &Rc<gix::Repository>,
    all_objects_ever_repo: &gix::Repository,
    namespace: &Namespace,
    mut pack_process: std::process::Child,
    refs: &HashMap<String, Ref>,
    force_refs: &HashMap<String, Option<Ref>>,
) -> Result<(Namespace, HashMap<String, bool>)> {
    let mut push_status = HashMap::new();

    let mut future = namespace.clone();

    let commit_cache = all_objects_ever_repo.commit_graph_if_enabled()?;
    let mut revision_graph = all_objects_ever_repo.revision_graph(commit_cache.as_ref());

    for (name, future_target) in refs.iter() {
        let ff = match namespace.refs.get(name) {
            Some(current_target) => can_fast_forward(
                all_objects_ever_repo,
                &mut revision_graph,
                name,
                current_target,
                future_target,
            )
            .context("failed to check fast forward")?,
            None => true,
        };

        if ff {
            future.refs.insert(name.clone(), future_target.clone());
        }

        push_status.insert(name.to_string(), ff);
    }

    for (name, future_target) in force_refs.iter() {
        push_status.insert(name.to_string(), true);
        match future_target {
            Some(future_target) => {
                future.refs.insert(name.clone(), future_target.clone());
            }
            None => {
                future.refs.remove(name);
            }
        }
    }

    let mut reader = std::io::BufReader::new(pack_process.stdout.take().context("No stdout.")?);
    let (blob_ref, size) = encode(
        tracking_repo,
        &mut reader,
        config.nacl_keys.namespace_key(),
        config.max_object_size,
    )
    .context("encode pack file")?;

    wait_subprocess(&mut pack_process).context("git pack-objects")?;

    if size > 0 {
        let random_name: [u8; 20] = rand::thread_rng().r#gen();
        let pack_ref = PackRef {
            blob_ref,
            random_name,
        };
        future.pack = Some(pack_ref);
    } else {
        future.pack = None;
    }

    Ok((future, push_status))
}

fn graph_descendant_of(
    graph: &mut gix_revision::Graph<()>,
    new: ObjectId,
    old: ObjectId,
) -> Result<bool> {
    if new == old {
        // This differs from `git_graph_descendant_of` in that we consider a
        // commit to be a descendant of itself.
        return Ok(true);
    }

    graph.clear();
    let mut stack = vec![new];
    let mut found = false;

    // This should only traverse each parent once, since `graph` is stateful and
    // calls the first closure for a new parent and the second for an existing
    // one.
    while let Some(id) = stack.pop() {
        graph.insert_parents(
            &id,
            &mut |pid, _ts| {
                if pid == old {
                    found = true;
                } else {
                    stack.push(pid);
                }
            },
            &mut |_pid, _d| {},
            /*first_parent=*/ false,
        )?;

        if found {
            return Ok(true);
        }
    }

    Ok(false)
}

fn can_fast_forward(
    all_objects_ever_repo: &gix::Repository,
    graph: &mut gix_revision::Graph<()>,
    name: &str,
    current: &Ref,
    future: &Ref,
) -> Result<bool> {
    if current == future {
        return Ok(true);
    }

    let make_message = |m| {
        format!(
            "Recursive Remote allows only a few types of push without force. Unable to push {}: {:?} -> {:?}: {}",
            name, current, future, &m
        )
    };
    match (current, future) {
        (Ref::Direct(old), Ref::Direct(new)) => {
            if name.starts_with("refs/tags/") {
                log::info!("{}", make_message("name starts with refs/tags/ and exists"));
                return Ok(false);
            }

            let old_kind = all_objects_ever_repo
                .find_object(*old)
                .with_context(|| format!("find current object kind for {}", old))?
                .kind;
            let new_kind = all_objects_ever_repo
                .find_object(*new)
                .with_context(|| format!("find future object kind for {}", new))?
                .kind;

            // Approximate git's push acceptance without shelling out: updates
            // to existing refs are only accepted without force when both
            // objects are commits and the update is a fast-forward.
            if old_kind != gix_object::Kind::Commit || new_kind != gix_object::Kind::Commit {
                log::info!(
                    "{}",
                    make_message("non-commit update requires force for existing refs")
                );
                return Ok(false);
            }

            if !graph_descendant_of(graph, *new, *old)? {
                log::info!("{}", make_message("not fast forward"));
                return Ok(false);
            }
        }
        (Ref::Symbolic(..), _) | (_, Ref::Symbolic(..)) => return Ok(false),
    }

    Ok(true)
}

/// Given the parent's commit root tree as a tree builder, create the new commit
/// tree.
pub fn create_commit_tree<'a>(
    repo: &gix::Repository,
    namespace_name: &str,
    mut root: gix::object::tree::Editor<'a>,
    tracking_repo: &Rc<gix::Repository>,
    state: &State,
    encrypt: &EncryptionKeys,
    max_object_size: usize,
) -> Result<ObjectId> {
    let namespace_ref = state
        .namespaces
        .get(namespace_name)
        .context("namespace should have been written already")?;
    let namespace = state
        .namespace(&namespace_name, encrypt, tracking_repo)?
        .expect("namespace should have been written already");

    let name = format!("ns_{}", hex::encode(namespace.random_name));

    let namespace_tree = create_treebuilder_at(tracking_repo, &root, &name)?;
    let namespace_tree =
        create_namespace_tree(tracking_repo, namespace_tree, &namespace, namespace_ref)?;

    root.upsert(&name, EntryKind::Tree, namespace_tree)
        .with_context(|| format!("insert namespace tree for namespace {}", namespace_name))?;

    let oids = match encode_state(tracking_repo, state, encrypt, max_object_size)
        .context("encode state.bincode")?
        .resource_key
    {
        ResourceKey::Git(oids) => oids,
        _ => unreachable!(),
    };

    // This is the "root" state for the current commit.
    insert_metadata_chunk_tree(&repo, &mut root, "state", &oids).context("insert state.bincode")?;

    Ok(root.write()?.into())
}

fn create_treebuilder_at<'a>(
    repo: &'a gix::Repository,
    parent: &gix::object::tree::Editor<'a>,
    name: &str,
) -> Result<gix::object::tree::Editor<'a>> {
    match parent.get(name) {
        Some(entry) => repo.find_tree(entry.id())?.edit(),
        None => repo.empty_tree().edit(),
    }
    .map_err(Into::into)
}

fn create_chunk_tree_or_blob<'a>(
    repo: &gix::Repository,
    oids: &[ObjectId],
) -> Result<Option<(ObjectId, EntryKind)>> {
    if oids.is_empty() {
        return Ok(None);
    }

    if oids.len() == 1 {
        return Ok(Some((oids[0], EntryKind::Blob)));
    }

    let mut tree = repo.empty_tree().edit().unwrap();

    for (i, oid) in oids.iter().enumerate() {
        tree.upsert(format!("{:08}", i), EntryKind::Blob, *oid)?;
    }

    Ok(Some((tree.write()?.into(), EntryKind::Tree)))
}

fn insert_metadata_chunk_tree<'a>(
    repo: &gix::Repository,
    root: &mut gix::object::tree::Editor<'a>,
    name: &str,
    oids: &[ObjectId],
) -> Result<()> {
    let (oid, mode) = create_chunk_tree_or_blob(repo, oids)?.context("empty metadata")?;

    // Don't forget you're here forever.
    let mut forever_tree = create_treebuilder_at(repo, &root, name)?;
    let forever_name: [u8; 20] = rand::thread_rng().r#gen();
    insert_into_name_tree(&mut forever_tree, forever_name, oid, mode)?;
    root.upsert(name, EntryKind::Tree, forever_tree.write()?)?;
    root.upsert(&format!("{name}.bincode"), mode.into(), oid)?;

    Ok(())
}

pub fn create_namespace_tree<'a>(
    tracking_repo: &'a gix::Repository,
    mut root: gix::object::tree::Editor<'a>,
    namespace: &Namespace,
    namespace_ref: &NamespaceRef,
) -> Result<ObjectId> {
    let oids = match &namespace_ref.0.resource_key {
        ResourceKey::Git(oids) => oids,
        _ => unreachable!(),
    };

    insert_metadata_chunk_tree(tracking_repo, &mut root, "namespace", &oids)
        .context("insert namespace.bincode")?;

    if let Some(pack) = namespace.pack.as_ref() {
        match &pack.blob_ref.resource_key {
            ResourceKey::Git(oids) => match create_chunk_tree_or_blob(tracking_repo, &oids)? {
                Some((oid, mode)) => {
                    let mut pack_tree = create_treebuilder_at(tracking_repo, &root, "pack")?;
                    insert_into_name_tree(&mut pack_tree, pack.random_name, oid, mode)?;
                    root.upsert("pack", EntryKind::Tree, pack_tree.write()?)?;
                }
                _ => {}
            },
            ResourceKey::Annex(..) => {
                unreachable!();
            }
        }
    }

    Ok(root.write()?.into())
}

#[derive(Clone, Copy)]
struct NameTreePathComponents<'a>(&'a [u8; 40]);

#[derive(Clone, Copy)]
struct NameTreePathComponentsIterator<'a>(&'a [u8]);

impl<'a> gix::object::tree::editor::ToComponents for NameTreePathComponents<'a> {
    fn to_components(&self) -> impl Iterator<Item = &BStr> {
        NameTreePathComponentsIterator(self.0)
    }
}

impl<'a> Iterator for NameTreePathComponentsIterator<'a> {
    type Item = &'a BStr;
    fn next(&mut self) -> Option<Self::Item> {
        match self.0.len() {
            40 | 38 => {
                let (car, cdr) = self.0.split_at(2);
                self.0 = cdr;
                Some(car.into())
            }
            36 => {
                let car = self.0;
                self.0 = &[];
                Some(car.into())
            }
            _ => None,
        }
    }
}

fn insert_into_name_tree<'a>(
    root: &mut gix::object::tree::Editor<'a>,
    name: [u8; 20],
    value: ObjectId,
    mode: EntryKind,
) -> Result<()> {
    let hex_name = hex::encode(name);
    let hex_name_slice: &[u8; 40] = &hex_name.as_bytes().try_into().expect("name length");
    let path = NameTreePathComponents(hex_name_slice);
    root.upsert(path, mode, value)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_repo_with_linear_history() -> (tempfile::TempDir, gix::Repository, ObjectId, ObjectId)
    {
        let tmp = tempfile::Builder::new()
            .prefix("persistence-tests")
            .tempdir()
            .expect("tempdir");
        let repo = gix::init_bare(tmp.path().join("repo")).expect("init bare repo");
        let tree = repo
            .empty_tree()
            .edit()
            .expect("empty tree")
            .write()
            .expect("tree oid");
        let c1 = anyhow_ref_commit(&repo, "refs/heads/test", "Recursive.", tree.into())
            .expect("commit 1");
        let c2 = anyhow_ref_commit(&repo, "refs/heads/test", "Recursive.", tree.into())
            .expect("commit 2");
        (tmp, repo, c1, c2)
    }

    #[test]
    fn can_fast_forward_accepts_commit_fast_forward() {
        let (_tmp, repo, c1, c2) = setup_repo_with_linear_history();
        let commit_cache = repo.commit_graph_if_enabled().expect("commit graph");
        let mut graph = repo.revision_graph(commit_cache.as_ref());

        let ok = can_fast_forward(
            &repo,
            &mut graph,
            "refs/heads/main",
            &Ref::Direct(c1),
            &Ref::Direct(c2),
        )
        .expect("can_fast_forward");
        assert!(ok);
    }

    #[test]
    fn can_fast_forward_rejects_non_fast_forward() {
        let (_tmp, repo, c1, c2) = setup_repo_with_linear_history();
        let commit_cache = repo.commit_graph_if_enabled().expect("commit graph");
        let mut graph = repo.revision_graph(commit_cache.as_ref());

        let ok = can_fast_forward(
            &repo,
            &mut graph,
            "refs/heads/main",
            &Ref::Direct(c2),
            &Ref::Direct(c1),
        )
        .expect("can_fast_forward");
        assert!(!ok);
    }

    #[test]
    fn can_fast_forward_rejects_tag_update_without_force() {
        let (_tmp, repo, c1, c2) = setup_repo_with_linear_history();
        let commit_cache = repo.commit_graph_if_enabled().expect("commit graph");
        let mut graph = repo.revision_graph(commit_cache.as_ref());

        let ok = can_fast_forward(
            &repo,
            &mut graph,
            "refs/tags/v1",
            &Ref::Direct(c1),
            &Ref::Direct(c2),
        )
        .expect("can_fast_forward");
        assert!(!ok);
    }

    #[test]
    fn can_fast_forward_rejects_symbolic_refs() {
        let (_tmp, repo, c1, _c2) = setup_repo_with_linear_history();
        let commit_cache = repo.commit_graph_if_enabled().expect("commit graph");
        let mut graph = repo.revision_graph(commit_cache.as_ref());

        let ok = can_fast_forward(
            &repo,
            &mut graph,
            "refs/heads/main",
            &Ref::Symbolic("refs/heads/other".to_string(), None),
            &Ref::Direct(c1),
        )
        .expect("can_fast_forward");
        assert!(!ok);
    }

    #[test]
    fn can_fast_forward_rejects_non_commit_targets() {
        let (_tmp, repo, c1, _c2) = setup_repo_with_linear_history();
        let blob_id = repo.write_blob("not a commit").expect("write blob");
        let commit_cache = repo.commit_graph_if_enabled().expect("commit graph");
        let mut graph = repo.revision_graph(commit_cache.as_ref());

        let ok = can_fast_forward(
            &repo,
            &mut graph,
            "refs/heads/main",
            &Ref::Direct(c1),
            &Ref::Direct(blob_id.into()),
        )
        .expect("can_fast_forward");
        assert!(!ok);
    }
}
