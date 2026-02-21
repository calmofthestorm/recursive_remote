use std::collections::HashMap;
use std::io::Write;
use std::rc::Rc;

use anyhow::{Context, Result};
use gix::diff::object::FindHeader;

use crate::config::*;
use crate::persistence::*;
use crate::serialization::*;
use crate::update::*;
use crate::util::*;

enum PushResult {
    Ok(HashMap<String, bool>),
    Retry,
}

fn classify_failed_push_for_retry(
    previous_state_identifier: &Option<StateRef>,
    new_state_identifier: &Option<StateRef>,
    push_error: &anyhow::Error,
) -> Result<PushResult> {
    if new_state_identifier != previous_state_identifier {
        log::info!("Unable to push and upstream has changed from");
        Ok(PushResult::Retry)
    } else {
        None.with_context(|| {
            format!(
                "Unable to push and upstream has not changed: {:?}",
                push_error
            )
        })
    }
}

fn parse_push_specs(
    specs: &Vec<String>,
    pushes: &mut Vec<(String, String)>,
    force_pushes: &mut Vec<(String, String)>,
) -> Result<()> {
    for spec in specs {
        let tok: Vec<_> = spec.split_ascii_whitespace().collect();
        if tok.len() != 2 || tok[0] != "push" {
            return None.context("not two tokens or not start with push").into();
        }
        let mut tok = tok[1].split(":");
        let source = tok.next().context("bad push spec source")?.to_string();
        let dest = tok.next().context("bad push spec dest")?.to_string();
        match source.chars().next() {
            Some('+') => force_pushes.push((source[1..].to_string(), dest)),
            None => force_pushes.push((source, dest)),
            _ => pushes.push((source, dest)),
        }
    }
    Ok(())
}

fn start_pack_process(
    user_repo: &Rc<gix::Repository>,
    namespace: &Namespace,
    pushes: &HashMap<String, Ref>,
    force_pushes: &HashMap<String, Option<Ref>>,
    shallow_basis: &Vec<Ref>,
) -> Result<std::process::Child> {
    let mut cmd = crate::util::git_command()
        .arg("pack-objects")
        .arg("--revs")
        .arg("--thin")
        .arg("--stdout")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn git pack-objects.")?;

    let stdin = cmd.stdin.take().context("No stdin.")?;
    let mut stdin = std::io::BufWriter::new(stdin);

    // INCLUDE revs reachable from the push spec.
    for oid in pushes
        .values()
        .chain(force_pushes.values().filter_map(Option::as_ref))
        .filter_map(|r| r.oid_at_time())
    {
        write!(&mut stdin, "{}\n", oid).context("write include revs to git pack-objects")?;
    }

    // EXCLUDE revs that are already in the upstream repository and those in the
    // shallow basis.
    for oid in namespace
        .refs
        .values()
        .chain(shallow_basis.iter())
        .filter_map(|r| r.oid_at_time())
    {
        // We must skip excluding refs that are absent in the repo. This is
        // unusual but not impossible, if the user garbage collected some
        // things or whatnot. Arguably git pack-objects could just ignore
        // them, but it does not.
        if let Ok(result) = user_repo.objects.try_header(oid.as_ref()) {
            if result.is_some() {
                write!(&mut stdin, "^{}\n", oid)
                    .context("write exclude revs to git pack-objects")?;
            }
        }
    }

    Ok(cmd)
}

fn convert_force_specs_to_refs(
    user_repo: &gix::Repository,
    specs: &Vec<(String, String)>,
) -> Result<HashMap<String, Option<Ref>>> {
    let mut refs = HashMap::new();
    for (target, dest) in specs {
        if target.is_empty() {
            refs.insert(dest.clone(), None);
            continue;
        }

        let reference = Ref::new(user_repo, target)
            .with_context(|| format!("resolve reference shortname {}", target))?;
        refs.insert(dest.clone(), Some(reference));
    }
    Ok(refs)
}

fn convert_specs_to_refs(
    user_repo: &gix::Repository,
    specs: &Vec<(String, String)>,
) -> Result<HashMap<String, Ref>> {
    let refs = convert_force_specs_to_refs(user_repo, specs).context("parse")?;
    let mut refs_out = HashMap::new();
    for (name, target) in refs {
        refs_out.insert(
            name,
            match target {
                Some(target) => target,
                None => {
                    return None.context(
                        "logic error -- push deletes should always get lumped in with force",
                    );
                }
            },
        );
    }

    Ok(refs_out)
}

// Commits to the branch, using the current tip as a parent. Normally this is a
// bad pattern with git; we do so here because we have held a lock on these
// branches the entire time and want to handle the case where the existing
// commit lacks state.bincode, which we treat as lacking a logical parent (but it
// still needs a physical one for git).
fn do_commit(
    namespace_name: &str,
    tracking_repo: &Rc<gix::Repository>,
    local_ref: &str,
    future: &State,
    root_id: Option<gix_hash::ObjectId>,
    encrypt: &EncryptionKeys,
    max_object_size: usize,
) -> Result<()> {
    let root = match root_id {
        None => None,
        Some(oid) => {
            // We base each commit on the tree from the previous, if any. There
            // are two reasons we don't just re-create the tree: 1) we may not
            // have encryption key for other namespaces and 2) we don't actually
            // use the git object graph for anything except reachability and the
            // "root" case of resolving a reference to a commit (and thence to
            // state.bincode). There's also presumably a performance benefit.
            Some(
                tracking_repo
                    .find_tree(oid)
                    .context("find root tree for previous commit")?,
            )
        }
    };

    let root = match root {
        Some(root) => root.edit(),
        None => tracking_repo.empty_tree().edit(),
    }?;

    let tree = create_commit_tree(
        &tracking_repo,
        namespace_name,
        root,
        &tracking_repo,
        &future,
        encrypt,
        max_object_size,
    )
    .context("create commit tree")?;
    anyhow_ref_commit(tracking_repo, local_ref, "Recursive.", tree)
        .with_context(|| format!("failed to commit tree {} to ref {}", &tree, &local_ref))
        .map(|_| ())
}

fn attempt_push(
    config: &Config,
    pushes: &Vec<(String, String)>,
    force_pushes: &Vec<(String, String)>,
) -> Result<PushResult> {
    let (state_identifier, state, _basis_state, root_id, _commit_id) =
        update_branches(config).context("push")?;

    let namespace = {
        let tracking_repo = Rc::new(config.tracking_repo()?);
        state
            .namespace(&config.namespace, &config.nacl_keys, &tracking_repo)?
            .unwrap_or_else(Namespace::new)
    };

    let user_repo = Rc::new(config.user_repo()?);

    let pushes = convert_specs_to_refs(&user_repo, pushes).context("pushes")?;
    let force_pushes: HashMap<_, _> =
        convert_force_specs_to_refs(&user_repo, force_pushes).context("force pushes")?;

    // Transfer the refs to the all objects repo. We do this rather than packing
    // in the user repo because it helps guard against races between refs and
    // objects. In theory, we could just eliminate them, but not in practice.
    //
    // This then acts as a safeguard in that we cannot commit corrupt state. We
    // don't assume any particular locking on the user repo, but we lock our own
    // exclusively.
    let mut cmd = crate::util::git_command();
    cmd.arg("push")
        .arg(&config.all_objects_ever_repo_path)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    for rev in pushes
        .values()
        .chain(force_pushes.values().filter_map(Option::as_ref))
        .filter_map(|r| r.oid_at_time())
    {
        let tmp_ref = format!("refs/recursive_remote/{}/tmp/tmp{rev}", &config.remote_name);
        user_repo
            .reference(
                tmp_ref.as_str(),
                rev,
                gix_ref::transaction::PreviousValue::Any,
                "Recursive",
            )
            .with_context(|| format!("create tmp ref in user_repo -> {}", &rev))?;
        cmd.arg(format!(
            "{tmp_ref}:refs/heads/{}/rev{rev}",
            &config.remote_name
        ));
    }

    execute_subprocess2(&mut cmd).context("git push to all objects repo")?;

    // Clean up the temporary refs for this remote from any previous ops, since
    // we do have a per-remote lock.
    crate::cmd_fetch::delete_refs_glob(
        &user_repo,
        &format!("refs/recursive_remote/{}/tmp/", &config.remote_name),
    )?;

    let all_objects_ever_repo = Rc::new(config.all_objects_ever_repo()?);
    let pack_process = start_pack_process(
        &all_objects_ever_repo,
        &namespace,
        &pushes,
        &force_pushes,
        &config.shallow_basis,
    )
    .context("start pack revs process")?;

    let tracking_repo = Rc::new(config.tracking_repo()?);
    let (future_namespace, push_status) = update_namespace_with_push(
        config,
        &tracking_repo,
        &all_objects_ever_repo,
        &namespace,
        pack_process,
        &pushes,
        &force_pushes,
    )
    .context("update_namespace_with_push")?;
    let future = update_state_with_push(
        config,
        &tracking_repo,
        &state,
        &future_namespace,
        &state_identifier,
    )
    .context("update_state_with_push")?;

    do_commit(
        &config.namespace,
        &tracking_repo,
        &config.pushing_ref,
        &future,
        root_id,
        &config.nacl_keys,
        config.max_object_size,
    )
    .context("commit")?;

    let push_result = execute_subprocess2(
        crate::util::git_command()
            .env("GIT_DIR", &config.tracking_repo_path)
            .arg("push")
            .arg(&config.remote_name)
            .arg(&format!("{}:{}", &config.pushing_ref, &config.remote_ref))
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()),
    )
    .context("git push");

    if push_result.is_ok() {
        return Ok(PushResult::Ok(push_status));
    }

    let (new_state_identifier, _new_state, _basis_state, _root_id, _commit_id) =
        update_branches(config).context("push secondary update")?;
    classify_failed_push_for_retry(
        &state_identifier,
        &new_state_identifier,
        &push_result.expect_err("checked is_ok above"),
    )
}

pub fn push(config: &Config, specs: &Vec<String>) -> Result<()> {
    let mut pushes = Vec::new();
    let mut force_pushes = Vec::new();
    parse_push_specs(specs, &mut pushes, &mut force_pushes)
        .with_context(|| format!("parse push specs: {:?}", specs))?;

    for _ in 0..25 {
        let success = attempt_push(config, &pushes, &force_pushes)?;

        match success {
            PushResult::Ok(push_status) => {
                for (name, status) in push_status {
                    if status {
                        log::trace!("push {} status: OK", &name);
                        println!("ok {}\n", &name);
                    } else {
                        log::trace!("push {} status: rejected", &name);
                        println!("error {} rejected\n", &name);
                    }
                }

                return Ok(());
            }
            PushResult::Retry => {}
        }
    }

    None.context("After many tries, unable to push due to conflicts in the backing repo.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_state_ref(tag: u8) -> StateRef {
        StateRef(BlobRef {
            resource_key: ResourceKey::Annex(format!("annex-{tag}")),
            sha256: [tag; 32],
        })
    }

    fn setup_repo() -> (tempfile::TempDir, gix::Repository) {
        let tmp = tempfile::Builder::new()
            .prefix("cmd-push-tests")
            .tempdir()
            .expect("tempdir");
        let repo = gix::init_bare(tmp.path().join("repo")).expect("init bare repo");
        let tree = repo
            .empty_tree()
            .edit()
            .expect("edit")
            .write()
            .expect("write tree");
        anyhow_ref_commit(&repo, "refs/heads/main", "Recursive.", tree.into()).expect("commit");
        (tmp, repo)
    }

    #[test]
    fn parse_push_specs_rejects_non_push_lines() {
        let specs = vec!["fetch abc:def".to_string()];
        let mut pushes = Vec::new();
        let mut force_pushes = Vec::new();
        let err = parse_push_specs(&specs, &mut pushes, &mut force_pushes).expect_err("must fail");
        assert!(format!("{err}").contains("not two tokens or not start with push"));
    }

    #[test]
    fn parse_push_specs_rejects_missing_dest() {
        let specs = vec!["push refs/heads/main".to_string()];
        let mut pushes = Vec::new();
        let mut force_pushes = Vec::new();
        let err = parse_push_specs(&specs, &mut pushes, &mut force_pushes).expect_err("must fail");
        assert!(format!("{err}").contains("bad push spec dest"));
    }

    #[test]
    fn parse_push_specs_splits_force_and_regular_pushes() {
        let specs = vec![
            "push refs/heads/main:refs/heads/main".to_string(),
            "push +refs/tags/v1:refs/tags/v1".to_string(),
            "push :refs/heads/old".to_string(),
        ];
        let mut pushes = Vec::new();
        let mut force_pushes = Vec::new();
        parse_push_specs(&specs, &mut pushes, &mut force_pushes).expect("parse");

        assert_eq!(
            pushes,
            vec![("refs/heads/main".to_string(), "refs/heads/main".to_string())]
        );
        assert_eq!(
            force_pushes,
            vec![
                ("refs/tags/v1".to_string(), "refs/tags/v1".to_string()),
                ("".to_string(), "refs/heads/old".to_string())
            ]
        );
    }

    #[test]
    fn convert_force_specs_to_refs_keeps_deletes_as_none() {
        let (_tmp, repo) = setup_repo();
        let specs = vec![
            ("refs/heads/main".to_string(), "refs/heads/main".to_string()),
            ("".to_string(), "refs/heads/old".to_string()),
        ];
        let refs = convert_force_specs_to_refs(&repo, &specs).expect("convert");
        assert!(refs.get("refs/heads/main").expect("main").is_some());
        assert!(refs.get("refs/heads/old").expect("old").is_none());
    }

    #[test]
    fn convert_specs_to_refs_rejects_delete_without_force() {
        let (_tmp, repo) = setup_repo();
        let specs = vec![("".to_string(), "refs/heads/old".to_string())];
        let err = convert_specs_to_refs(&repo, &specs).expect_err("must fail");
        assert!(format!("{err}").contains("push deletes should always get lumped in with force"));
    }

    #[test]
    fn classify_failed_push_for_retry_returns_retry_when_state_changes() {
        let old = Some(fake_state_ref(1));
        let new = Some(fake_state_ref(2));
        let err = anyhow::anyhow!("git push failed");
        let out = classify_failed_push_for_retry(&old, &new, &err).expect("result");
        match out {
            PushResult::Retry => {}
            PushResult::Ok(..) => panic!("expected retry"),
        }
    }

    #[test]
    fn classify_failed_push_for_retry_errors_when_state_unchanged() {
        let state = fake_state_ref(7);
        let old = Some(state.clone());
        let new = Some(state);
        let err = anyhow::anyhow!("git push failed");
        let out = classify_failed_push_for_retry(&old, &new, &err)
            .err()
            .expect("must fail");
        assert!(format!("{out}").contains("upstream has not changed"));
    }
}
