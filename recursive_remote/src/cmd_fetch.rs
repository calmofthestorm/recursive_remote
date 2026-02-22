use std::collections::HashSet;
use std::convert::TryInto;
use std::io::BufRead;
use std::rc::Rc;

use anyhow::{Context, Result};
use gix::Repository;

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
pub fn fetch(config: &Config, revs: &[String]) -> Result<()> {
    let (state_identifier, state, basis_ref, _root_id, commit_id) =
        update_branches(config).context("fetch")?;

    let tracking_repo = Rc::new(config.tracking_repo()?);

    let ordered_packs = materialize_ordered_pack_list(
        config,
        &tracking_repo,
        state_identifier.as_ref(),
        &state,
        basis_ref.as_ref(),
    )?;

    // Clean up the temporary refs for this remote from any previous ops, since
    // we do have a per-remote lock.
    delete_refs_glob(
        &config.user_repo()?,
        &format!("refs/recursive_remote/{}/tmp/", &config.remote_name),
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

    let fetch_revs = parse_fetch_revs(revs);

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
                    format!("refs/heads/{}/rev{}", &config.remote_name, &rev),
                    gix_hash::ObjectId::from_hex(rev.as_bytes()).context("oid")?,
                    gix_ref::transaction::PreviousValue::Any,
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
                config.basis_ref.as_str(),
                commit_id,
                gix_ref::transaction::PreviousValue::Any,
                "Recursive",
            )
            .context("update basis ref")?;
    }

    // Ensure that all objects ever downloaded remain reachable.
    compact_ref_reachability(&all_objects_ever_repo, &config.remote_name)?;

    println!();

    Ok(())
}

pub fn materialize_ordered_pack_list(
    config: &Config,
    tracking_repo: &Rc<gix::Repository>,
    state_identifier: Option<&StateRef>,
    state: &State,
    basis_ref: Option<&StateRef>,
) -> Result<Vec<PackRef>> {
    let mut stack = vec![(state_identifier.cloned(), Some(state))];

    let mut ordered_packs = Vec::default();
    while let Some((state_identifier, state)) = stack.pop() {
        if let (Some(basis_ref), Some(state_identifier)) = (basis_ref, state_identifier.as_ref())
            && basis_ref == state_identifier
        {
            continue;
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
            .namespace(&config.namespace, &config.nacl_keys, tracking_repo)?
            .context("traverse")?;

        ordered_packs.extend(namespace.pack);

        for parent in state.parents.iter() {
            stack.push((Some(parent.clone()), None));
        }
    }
    Ok(ordered_packs)
}

fn compact_ref_reachability(repo: &gix::Repository, remote_name: &str) -> Result<()> {
    let mut ref_commits = Vec::default();
    let mut ref_names: Vec<String> = Vec::default();
    let prefix = format!("refs/heads/{}", remote_name);
    for mut r in repo.references()?.prefixed(prefix.as_str())? {
        match r {
            Err(e) => {
                anyhow::bail!(format!("error: {}", &e));
            }
            Ok(ref mut r) => {
                ref_commits.push(r.peel_to_commit()?.id);
                ref_names.push(r.name().as_bstr().to_string());
            }
        }
    }
    if ref_commits.len() > 50 {
        let uuidv4 = uuid::Uuid::new_v4();
        let tree = repo.empty_tree().edit()?.write()?;
        let sig = rr_signature();
        let mut committer_time = gix_date::parse::TimeBuf::default();
        let mut author_time = gix_date::parse::TimeBuf::default();
        repo.commit_as(
            sig.to_ref(&mut committer_time),
            sig.to_ref(&mut author_time),
            format!("refs/heads/{}/rev{uuidv4}", remote_name),
            "Recursive",
            tree,
            ref_commits,
        )?;
        for rev in ref_names.iter() {
            repo.find_reference(rev)?.delete()?;
        }
    }
    Ok(())
}

fn parse_fetch_revs(revs: &[String]) -> HashSet<String> {
    let mut fetch_revs = HashSet::new();
    for rev in revs {
        let mut tok = rev.split_ascii_whitespace();
        if tok.next() != Some("fetch") {
            continue;
        }
        let Some(oidish) = tok.next() else {
            continue;
        };
        let end = std::cmp::min(40, oidish.len());
        if end > 0 {
            fetch_revs.insert(oidish[..end].to_string());
        }
    }
    fetch_revs
}

pub fn delete_refs_glob(repo: &gix::Repository, glob: &str) -> Result<()> {
    // gix no longer accepts wildcard components here; treat '*' suffix as legacy syntax.
    let prefix = glob.trim_end_matches('*');
    for reference in repo.references()?.prefixed(prefix)? {
        match reference {
            Ok(reference) => {
                reference.delete()?;
            }
            Err(e) => anyhow::bail!("bad error: {}", &e),
        }
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
        tracking_repo,
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

    if let Some(line) = std::io::BufReader::new(stdout).lines().next() {
        let line = line?;
        let tok: Vec<_> = line.split_ascii_whitespace().collect();
        if tok.len() != 2 {
            anyhow::bail!("expected a line like 'keep <packname>'");
        }
        let name = hex::decode(tok[1]).context("decode hex written pack name")?;
        return Ok(Some(name.try_into().expect("")));
    }

    anyhow::bail!("no pack was written");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_repo() -> (tempfile::TempDir, gix::Repository, gix_hash::ObjectId) {
        let tmp = tempfile::Builder::new()
            .prefix("cmd-fetch-tests")
            .tempdir()
            .expect("tempdir");
        let repo = gix::init_bare(tmp.path().join("repo")).expect("init bare repo");
        let tree = repo
            .empty_tree()
            .edit()
            .expect("edit")
            .write()
            .expect("write tree");
        let commit =
            anyhow_ref_commit(&repo, "refs/heads/main", "Recursive.", tree.into()).expect("commit");
        (tmp, repo, commit)
    }

    fn count_prefixed_refs(repo: &gix::Repository, prefix: &str) -> usize {
        repo.references()
            .expect("refs")
            .prefixed(prefix)
            .expect("prefixed")
            .count()
    }

    #[test]
    fn parse_fetch_revs_ignores_non_fetch_and_handles_short_hashes() {
        let revs = vec![
            "capabilities".to_string(),
            "fetch abcdef".to_string(),
            "fetch 0123456789012345678901234567890123456789 refs/heads/main".to_string(),
            "fetch".to_string(),
        ];
        let parsed = parse_fetch_revs(&revs);
        assert!(parsed.contains("abcdef"));
        assert!(parsed.contains("0123456789012345678901234567890123456789"));
        assert_eq!(parsed.len(), 2);
    }

    #[test]
    fn delete_refs_glob_deletes_only_matching_prefix() {
        let (_tmp, repo, commit) = setup_repo();
        repo.reference(
            "refs/recursive_remote/origin/tmp/tmp1",
            commit,
            gix_ref::transaction::PreviousValue::Any,
            "Recursive",
        )
        .expect("tmp1");
        repo.reference(
            "refs/recursive_remote/origin/tmp/tmp2",
            commit,
            gix_ref::transaction::PreviousValue::Any,
            "Recursive",
        )
        .expect("tmp2");
        repo.reference(
            "refs/recursive_remote/origin/keep/me",
            commit,
            gix_ref::transaction::PreviousValue::Any,
            "Recursive",
        )
        .expect("keep");

        delete_refs_glob(&repo, "refs/recursive_remote/origin/tmp/*").expect("delete");

        assert_eq!(
            count_prefixed_refs(&repo, "refs/recursive_remote/origin/tmp/"),
            0
        );
        assert_eq!(
            count_prefixed_refs(&repo, "refs/recursive_remote/origin/keep/"),
            1
        );
    }

    #[test]
    fn compact_ref_reachability_compacts_when_many_refs() {
        let (_tmp, repo, commit) = setup_repo();
        for i in 0..51 {
            repo.reference(
                format!("refs/heads/origin/rev{i:02}"),
                commit,
                gix_ref::transaction::PreviousValue::Any,
                "Recursive",
            )
            .expect("reference");
        }
        assert_eq!(count_prefixed_refs(&repo, "refs/heads/origin"), 51);

        compact_ref_reachability(&repo, "origin").expect("compact");

        assert_eq!(count_prefixed_refs(&repo, "refs/heads/origin"), 1);
    }
}
