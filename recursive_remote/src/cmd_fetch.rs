use std::collections::HashSet;
use std::convert::TryInto;
use std::io::BufRead;
use std::rc::Rc;

use anyhow::{Context, Result};
use git2::Repository;

use crate::config::*;
use crate::encoding::*;
use crate::serialization::*;
use crate::update::*;
use crate::util::*;

// revs is mostly ignored in this function because we use the semantics that
// whenever you fetch anything, we fetch everything not currently in the repo
// that is in the remote.
//
// Much like double storing upstream as a local checkout, this is a time-space
// tradeoff. It is rare that I want some branches but not others (or at least
// that I care as a matter of disk space), and this makes things simpler.
//
// It would be possible to create a per-ref history graph, and then traverse
// that instead. The same principle applies.
//
// The reason for the strange song and dance with concatenate and fix_thin repos
// is to fetch multiple thin packs, fix them (which requires deltas from each
// other and the base repo cloned), then repack them all into one big pack,
// since that's how the special remote protocol prefers to handle locking.
pub fn fetch(config: &Config, revs: &Vec<String>) -> Result<()> {
    let (state_identifier, state, basis_ref, _root_id, commit_id) =
        update_branches(config).context("fetch")?;

    let tracking_repo = Rc::new(config.tracking_repo()?);

    let ordered_packs = materialize_ordered_pack_list(
        &config,
        &tracking_repo,
        state_identifier.as_ref(),
        &state,
        basis_ref.as_ref(),
    )?;

    // Clean up the temporary refs for this remote from any previous ops, since
    // we do have a per-remote lock.
    delete_refs_glob(
        &config.user_repo()?,
        &format!("refs/recursive_remote/{}/tmp/*", &config.remote_name),
    )?;

    // Fix the thin packs, and insert their objects into the all objects repo.
    for pack_ref in ordered_packs.iter().rev() {
        fetch_pack(config, &tracking_repo, pack_ref.clone())?;
    }

    // We want to keep all refs reachable so no objects are ever gc'd (.keep,
    // gc.pruneExpire=never, gc.cruftPacks, etc all do similar things, but each
    // has downsides), mostly due to being intended to eventually get rid of
    // objects -- e.g., git tries to keep only a single cruft pack.
    //
    // This is per-remote since our exclusive locking is.
    let all_objects_ever_repo = config.all_objects_ever_repo()?;

    let mut fetch_revs = HashSet::new();
    for rev in revs {
        if rev.starts_with("fetch ") {
            let rev = &rev["fetch ".len()..];
            let rev = &rev[..40];
            fetch_revs.insert(rev);
        }
    }

    if !fetch_revs.is_empty() {
        let mut cmd = crate::util::git_command();
        cmd.arg("fetch")
            .arg(&config.all_objects_ever_repo_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        for rev in fetch_revs {
            all_objects_ever_repo
                .reference(
                    &format!("refs/heads/{}/rev{}", &config.remote_name, &rev),
                    git2::Oid::from_str(rev).context("oid")?,
                    /*force=*/ true,
                    "Recursive",
                )
                .with_context(|| format!("create ref in all_objects_ever_repo -> {}", &rev))?;

            cmd.arg(format!(
                "refs/heads/{}/rev{rev}:refs/recursive_remote/{}/tmp/tmp{rev}",
                &config.remote_name, &config.remote_name
            ));
        }

        execute_subprocess2(&mut cmd).context("git fetch")?;
    }

    if let Some(commit_id) = commit_id {
        config
            .tracking_repo()?
            .reference(
                &config.basis_ref,
                commit_id,
                /*force=*/ true,
                "Recursive",
            )
            .context("update basis ref")?;
    }

    // Ensure that all objects ever downloaded remain reachable.
    compact_ref_reachability(&all_objects_ever_repo, &config.remote_name)?;

    println!("");

    Ok(())
}

pub fn materialize_ordered_pack_list(
    config: &Config,
    tracking_repo: &Rc<git2::Repository>,
    state_identifier: Option<&StateRef>,
    state: &State,
    basis_ref: Option<&StateRef>,
) -> Result<Vec<PackRef>> {
    let mut stack = vec![(state_identifier.map(Clone::clone), Some(state))];

    let mut ordered_packs = Vec::default();
    while let Some((state_identifier, state)) = stack.pop() {
        if let (Some(basis_ref), Some(state_identifier)) = (basis_ref, state_identifier.as_ref()) {
            if basis_ref == state_identifier {
                continue;
            }
        }

        let mut _sh = None;
        let state = match state.as_ref() {
            Some(state) => state,
            None => match state_identifier {
                Some(state_identifier) => {
                    _sh = Some(crate::encoding::decode_state(
                        tracking_repo,
                        &state_identifier,
                        &config.nacl_keys,
                    )?);
                    _sh.as_ref().expect("")
                }
                None => {
                    return Ok(Vec::default());
                }
            },
        };

        let namespace = state
            .namespace(&config.namespace, &config.nacl_keys, &tracking_repo)?
            .context("traverse")?;

        ordered_packs.extend(namespace.pack);

        for parent in state.parents.iter() {
            stack.push((Some(parent.clone()), None));
        }
    }
    Ok(ordered_packs)
}

fn compact_ref_reachability(repo: &git2::Repository, remote_name: &str) -> Result<()> {
    let mut ref_commits = Vec::default();
    let mut ref_names = Vec::default();
    for r in repo.references_glob(&format!("refs/heads/{}/*", remote_name))? {
        let r = r?;
        if let Some(name) = r.name() {
            ref_commits.push(r.peel_to_commit()?);
            ref_names.push(name.to_string());
        }
    }
    if ref_commits.len() > 50 {
        let tree = repo.treebuilder(None)?.write()?;
        let tree = repo.find_tree(tree)?;
        let sig = repo.signature()?;
        let parents: Vec<_> = ref_commits.iter().collect();
        let rev = repo.commit(None, &sig, &sig, "Recursive", &tree, &parents)?;
        repo.reference(
            &format!("refs/heads/{}/rev{rev}", remote_name),
            rev,
            /*force=*/ true,
            "Recursive",
        )
        .context("update tracking ref")?;
        for rev in ref_names.iter() {
            repo.find_reference(&rev)?.delete()?;
        }
    }
    Ok(())
}

pub fn delete_refs_glob(repo: &git2::Repository, glob: &str) -> Result<()> {
    for reference in repo.references_glob(glob)? {
        let mut reference = reference?;
        reference.delete()?;
    }
    Ok(())
}

pub fn fetch_pack(
    config: &Config,
    tracking_repo: &Rc<Repository>,
    pack_ref: PackRef,
) -> Result<Option<[u8; 20]>> {
    let mut cmd = crate::util::git_command()
        .current_dir(&config.all_objects_ever_repo_path)
        .arg("index-pack")
        .arg("--fix-thin")
        .arg("--stdin")
        .arg("--keep")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn git pack-objects.")?;

    let stdin = cmd.stdin.take().context("No stdin.")?;
    let stdout = cmd.stdout.take().context("No stdout.")?;

    let (_blob_ref, size) = decode(
        &tracking_repo,
        &pack_ref.blob_ref,
        stdin,
        config.nacl_keys.namespace_key(),
    )
    .context("decode pack")?;

    let r = wait_subprocess(&mut cmd).context("git index-pack");
    if r.is_err() {
        // Ideally we'd not spawn the process for an empty pack, but we don't
        // know the size until we've sunk the whole contents into the process
        // anyway. So just drop it so we ignore it dying with error on
        // unexpected EOF for an empty pack.
        //
        // We also try to handle this on the "push" side by setting the pack to
        // None if it would be empty.
        if size > 0 {
            r.with_context(|| format!("pack {} with size {}", pack_ref.blob_ref, size))?;
            unreachable!()
        } else {
            return Ok(None);
        }
    }

    for line in std::io::BufReader::new(stdout).lines() {
        let line = line?;
        let tok: Vec<_> = line.split_ascii_whitespace().collect();
        if tok.len() != 2 {
            anyhow::bail!("expected a line like 'keep <packname>'");
        }
        let name = hex::decode(&tok[1]).context("decode hex written pack name")?;
        return Ok(Some(name.try_into().expect("")));
    }

    anyhow::bail!("no pack was written");
}
