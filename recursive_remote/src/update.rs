use std::rc::Rc;

use anyhow::{Context, Result};
use git2::Oid;
use log::trace;
use record_reader::HashError;
use thiserror::Error;

use crate::config::{Config, EncryptionKeys};
use crate::encoding;
use crate::persistence::*;
use crate::serialization::{State, StateRef};
use crate::util::*;

#[derive(Error, Debug)]
pub enum RatchetError {
    #[error("ratchet error")]
    RatchetError,
}

// Force fetches the remote ref into `pushing_ref`, then validates that it is a
// fast-forward from the current `tracking_ref` using the sha256 inner hash
// structure. If not, this is an error. Otherwise, fetches into `tracking_ref`.`
//
// We use the inner structure rather than git history to enforce continuous
// logical history while allowing for rewriting git history to remove temporary
// artifacts.
pub fn update_branches(
    config: &Config,
) -> Result<(
    Option<StateRef>,
    State,
    Option<StateRef>,
    Option<Oid>,
    Option<Oid>,
)> {
    log::trace!("Fetching pushing branch from underlying remote.");
    update_pushing_branch(config).context("pushing tracking branch")?;

    log::trace!("Checking ratchet properties to update tracking branch.");
    update_tracking_branch(config).context("update tracking branch")
}

fn update_pushing_branch(config: &Config) -> Result<()> {
    {
        // Delete the local branch before proceeding. We do this so that if
        // there is no upstream branch we start clean.
        let tracking_repo = config.tracking_repo()?;
        if let Ok(mut r) = tracking_repo.find_reference(&config.pushing_ref) {
            r.delete().context("delete reference")?;
        };
    }

    let ls_output = execute_subprocess2(
        crate::util::git_command()
            .env("GIT_DIR", &config.tracking_repo_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .arg("ls-remote")
            .arg(&config.remote_name)
            .arg(&config.remote_ref),
    )?;

    for line in std::str::from_utf8(&ls_output.stdout)
        .context("decode utf-8 from git ls-remote")?
        .lines()
    {
        let tok: Vec<_> = line.split_ascii_whitespace().collect();
        if tok.len() != 2 {
            anyhow::bail!(
                "expected 2 tokens in git ls-remote output but got {:?}",
                &line
            );
        }
        let ref_name = tok[1];
        if ref_name == config.remote_ref {
            execute_subprocess2(
                crate::util::git_command()
                    .env("GIT_DIR", &config.tracking_repo_path)
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::piped())
                    .stderr(std::process::Stdio::piped())
                    .arg("fetch")
                    .arg(&config.remote_name)
                    .arg(&format!("+{}:{}", &config.remote_ref, &config.pushing_ref)),
            )
            .ok()
            .context("Failed to fetch underlying branch.")?;
            break;
        }
    }

    Ok(())
}

pub fn resolve_state_ref(
    tracking_repo: &Rc<git2::Repository>,
    keys: &EncryptionKeys,
    name: &str,
) -> Result<Option<(Oid, (StateRef, State), Oid)>> {
    Ok(match ref_to_state_oid(&tracking_repo, name)? {
        Some((commit_oid, root_oid, tree_oid)) => Some((
            commit_oid,
            crate::encoding::unverified::decode_unverified_state_from_tree_or_blob_oid(
                &tracking_repo,
                tree_oid,
                keys,
                /*want_sha256=*/ &None,
            )?,
            root_oid,
        )),
        None => None,
    })
}

fn update_tracking_branch(
    config: &Config,
) -> Result<(
    Option<StateRef>,
    State,
    Option<StateRef>,
    Option<Oid>,
    Option<Oid>,
)> {
    let tracking_repo = Rc::new(config.tracking_repo()?);

    let resolve = |name| resolve_state_ref(&tracking_repo, &config.nacl_keys, name);

    let cur_oid = resolve(&config.tracking_ref).context("get state oid for tracking ref")?;
    let fut_oid = resolve(&config.pushing_ref).context("get state oid for pushing ref")?;
    let bas_oid = resolve(&config.basis_ref).context("get state oid for basis ref")?;
    let bas_oid = bas_oid.map(|bas| bas.1 .0);

    if let (Some(cur), Some(fut)) = (cur_oid.as_ref(), fut_oid.as_ref()) {
        if !valid_path_exists(config, &tracking_repo, &(cur.1).0, &(fut.1).0)? {
            return Err(RatchetError::RatchetError.into());
        }
    }

    match fut_oid {
        Some((commit_oid, future, root_oid)) => {
            tracking_repo
                .reference(
                    &config.tracking_ref,
                    commit_oid,
                    /*force=*/ true,
                    "Recursive",
                )
                .context("update tracking ref")?;
            Ok((
                Some(future.0),
                future.1,
                bas_oid,
                Some(root_oid),
                Some(commit_oid),
            ))
        }
        None => {
            if let Ok(mut r) = tracking_repo.find_reference(&config.tracking_ref) {
                r.delete().context("delete tracking reference")?;
            };
            Ok((None, State::default(), bas_oid, None, None))
        }
    }
}

// Allow fast forward if either is undefined. This is different from
// git, since we are allowing unrelated history, but think of it as
// a trust-on-first-use chain.
//
// If we can reach `current_ident` from `future_ident`, accept it.
fn valid_path_exists(
    config: &Config,
    tracking_repo: &Rc<git2::Repository>,
    current: &StateRef,
    future: &StateRef,
) -> Result<bool> {
    let mut stack = vec![future.clone()];

    if current.0.sha256 == future.0.sha256 {
        return Ok(true);
    }

    trace!(
        "Seeking path back from future {} to current {}",
        hex::encode(future.0.sha256),
        hex::encode(current.0.sha256)
    );

    // This is permissive, since we only care about valid paths.
    while let Some(traverse) = stack.pop() {
        match encoding::decode_state(tracking_repo, &traverse, &config.nacl_keys) {
            Ok(state) if state.parents.contains(&current) => return Ok(true),
            Ok(mut state) => {
                stack.append(&mut state.parents);
            }
            Err(e) => match e.downcast_ref::<HashError>() {
                Some(..) => {}
                None => return Err(e).context("traverse sha256 history"),
            },
        }
    }

    log::warn!("Failed to find a path back.");

    Ok(false)
}
