use std::collections::HashMap;
use std::rc::Rc;

use anyhow::{Context, Result};
use git2::{Oid, Repository};
use rand::Rng;

use crate::config::{Config, EncryptionKeys};
use crate::encoding::*;
use crate::serialization::*;
use crate::util::*;

pub fn ref_to_state_oid(
    repo: &git2::Repository,
    ref_name: &str,
) -> Result<Option<(Oid, Oid, Oid)>> {
    Ok(match peel_reference_to_commit(repo, ref_name)? {
        Some(commit) => commit
            .tree()?
            .get_name("state.bincode")
            .map(|tree_entry| (commit.id(), commit.tree_id(), tree_entry.id())),
        None => None,
    })
}

// Updates the namespace in the state.
pub fn update_state_with_push(
    config: &Config,
    tracking_repo: &Rc<git2::Repository>,
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
    tracking_repo: &Rc<git2::Repository>,
    all_objects_ever_repo: &git2::Repository,
    namespace: &Namespace,
    mut pack_process: std::process::Child,
    refs: &HashMap<String, Ref>,
    force_refs: &HashMap<String, Option<Ref>>,
) -> Result<(Namespace, HashMap<String, bool>)> {
    let mut push_status = HashMap::new();

    let mut future = namespace.clone();

    for (name, future_target) in refs.iter() {
        let ff = match namespace.refs.get(name) {
            Some(current_target) => {
                can_fast_forward(all_objects_ever_repo, name, current_target, future_target)
                    .context("failed to check fast forward")?
            }
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
        let random_name: [u8; 20] = rand::thread_rng().gen();
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

fn can_fast_forward(
    user_repo: &git2::Repository,
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

            if !user_repo.graph_descendant_of(*new, *old)? {
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
    repo: &git2::Repository,
    namespace_name: &str,
    mut root: git2::TreeBuilder<'a>,
    tracking_repo: &Rc<git2::Repository>,
    state: &State,
    encrypt: &EncryptionKeys,
    max_object_size: usize,
) -> Result<Oid> {
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

    root.insert(&name, namespace_tree, git2::FileMode::Tree.into())
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

    Ok(root.write()?)
}

fn create_treebuilder_at<'a>(
    repo: &'a git2::Repository,
    parent: &git2::TreeBuilder<'a>,
    name: &str,
) -> Result<git2::TreeBuilder<'a>> {
    let tree = match parent.get(name)? {
        Some(entry) => Some(repo.find_tree(entry.id())?),
        None => None,
    };
    Ok(repo.treebuilder(tree.as_ref())?)
}

fn create_chunk_tree_or_blob<'a>(
    repo: &git2::Repository,
    oids: &[Oid],
) -> Result<Option<(Oid, git2::FileMode)>> {
    if oids.is_empty() {
        return Ok(None);
    }

    if oids.len() == 1 {
        return Ok(Some((oids[0], git2::FileMode::Blob)));
    }

    let mut tree = repo.treebuilder(None)?;

    for (i, oid) in oids.iter().enumerate() {
        tree.insert(format!("{:08}", i), *oid, git2::FileMode::Blob.into())?;
    }

    Ok(Some((tree.write()?, git2::FileMode::Tree)))
}

fn insert_metadata_chunk_tree<'a>(
    repo: &git2::Repository,
    root: &mut git2::TreeBuilder<'a>,
    name: &str,
    oids: &[Oid],
) -> Result<()> {
    let (oid, mode) = create_chunk_tree_or_blob(repo, oids)?.context("empty metadata")?;

    // Don't forget you're here forever.
    let mut forever_tree = create_treebuilder_at(repo, &root, name)?;
    let forever_name: [u8; 20] = rand::thread_rng().gen();
    insert_into_name_tree(repo, &mut forever_tree, forever_name, oid, mode)?;
    root.insert(name, forever_tree.write()?, git2::FileMode::Tree.into())?;
    root.insert(&format!("{name}.bincode"), oid, mode.into())?;

    Ok(())
}

pub fn create_namespace_tree<'a>(
    tracking_repo: &'a git2::Repository,
    mut root: git2::TreeBuilder<'a>,
    namespace: &Namespace,
    namespace_ref: &NamespaceRef,
) -> Result<Oid> {
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
                    insert_into_name_tree(
                        tracking_repo,
                        &mut pack_tree,
                        pack.random_name,
                        oid,
                        mode,
                    )?;
                    root.insert("pack", pack_tree.write()?, git2::FileMode::Tree.into())?;
                }
                _ => {}
            },
            ResourceKey::Annex(..) => {
                unreachable!();
            }
        }
    }

    Ok(root.write()?)
}

fn insert_into_name_tree<'a>(
    tracking_repo: &'a Repository,
    root: &mut git2::TreeBuilder<'a>,
    name: [u8; 20],
    value: Oid,
    mode: git2::FileMode,
) -> Result<()> {
    let name = hex::encode(name);
    let l1 = &name[0..2];
    let l2 = &name[2..4];
    let name = &name[4..40];
    let (old_l1, old_l2) = match root.get(&l1)? {
        Some(entry) => {
            let old_l1 = tracking_repo.find_tree(entry.id())?;
            // (Some(old_l1) , Some(old_l1) )
            let old_l2 = match old_l1.get_name(&l2) {
                Some(entry) => Some(tracking_repo.find_tree(entry.id())?),
                None => None,
            };
            (Some(old_l1), old_l2)
        }
        None => (None, None),
    };

    let mut l1t = tracking_repo.treebuilder(old_l1.as_ref())?;
    let mut l2t = tracking_repo.treebuilder(old_l2.as_ref())?;

    l2t.insert(name, value, mode.into())?;
    l1t.insert(l2, l2t.write()?, git2::FileMode::Tree.into())?;
    root.insert(l1, l1t.write()?, git2::FileMode::Tree.into())?;
    Ok(())
}
