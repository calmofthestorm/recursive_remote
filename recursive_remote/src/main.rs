use std::io::BufRead as _;
use std::path::PathBuf;
use std::rc::Rc;

use anyhow::{Context, Result};
use clap::App;
use log::{error, info, trace};

use recursive_remote::config::*;
use recursive_remote::serialization::Namespace;
use recursive_remote::update::*;
use recursive_remote::util::*;

include!(concat!(env!("OUT_DIR"), "/generated_stamp.rs"));

fn collect_lines<I>(lines: &mut I, key: &str, initial: Option<String>) -> Result<Vec<String>>
where
    I: Iterator<Item = Result<String, std::io::Error>>,
{
    let mut pushes = Vec::new();
    pushes.extend(initial);
    loop {
        match lines.next() {
            Some(Ok(line)) => {
                if line.split_ascii_whitespace().next().unwrap_or_default() == key {
                    pushes.push(line);
                } else if !line.trim().is_empty() {
                    None.context("expected blank line while collecting")?;
                } else {
                    return Ok(pushes);
                }
            }
            None => {
                error!("protocol error: expected line");
                return Ok(pushes);
            }
            Some(Err(e)) => return None.with_context(|| format!("Error: {:?}", &e)),
        }
    }
}

fn report_error(result: Result<()>) -> bool {
    match result {
        Ok(()) => true,
        Err(e) => {
            log::error!("Fatal error: {}", &e);
            false
        }
    }
}

fn initialize_state_repo(args: Args) -> Result<Config> {
    info!("Tracking repository is {:?}.", &args.tracking_repo_path);

    let all_ops_success = std::thread::scope(|scope| {
        let t1 = scope.spawn(|| {
            report_error((|| {
                {
                    let repo = args.tracking_repo()?;
                    let mut config = repo.config()?;
                    config.set_i64("gc.auto", 6700)?;
                    config.set_bool("gc.autoDetach", false)?;
                }
                git_gc_auto(&args.tracking_repo_path).context("git gc --auto")?;
                args.tracking_repo()?.config()?.set_i64("gc.auto", 0)?;
                Ok(())
            })())
        });

        let t2 = scope.spawn(|| {
            report_error((|| {
                {
                    let repo = args.push_semantics_repo()?;
                    let mut config = repo.config()?;
                    config.set_i64("gc.auto", 6700)?;
                    config.set_bool("gc.autoDetach", false)?;
                }
                git_gc_auto(&args.push_semantics_repo_path).context("git gc --auto")?;
                args.push_semantics_repo()?
                    .config()?
                    .set_i64("gc.auto", 0)?;

                Ok(())
            })())
        });

        let t3 = scope.spawn(|| {
            report_error((|| {
                {
                    let repo = args.all_objects_ever_repo()?;
                    let mut config = repo.config()?;
                    config.set_i64("gc.auto", 6700)?;
                    config.set_bool("gc.autoDetach", false)?;
                }

                // Clean up .keep files. In theory we no longer need to create
                // them during fetch.
                let pack_dir = args.all_objects_ever_repo_path.join("objects/pack");
                for fp in std::fs::read_dir(&pack_dir)
                    .with_context(|| format!("read pack_dir {}", pack_dir.display()))?
                {
                    let fp = fp.context("read dirent")?;
                    let fp = fp.path();
                    if let Some(ext) = fp.extension() {
                        if ext == "keep" {
                            std::fs::remove_file(&fp)
                                .with_context(|| format!("remove keep file {}", fp.display()))?;
                        }
                    }
                }

                git_gc_auto(&args.all_objects_ever_repo_path).context("git gc --auto")?;
                args.all_objects_ever_repo()?
                    .config()?
                    .set_i64("gc.auto", 0)?;

                Ok(())
            })())
        });

        t1.join().and(t2.join()).and(t3.join())
    });

    match all_ops_success {
        Ok(true) => Config::new(args),
        Ok(false) => {
            anyhow::bail!("An initialize threaded operation failed; see logs.");
        }
        Err(..) => {
            anyhow::bail!("Failed to join initialize threads.");
        }
    }
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace")).init();

    let pkg_name = env!("CARGO_PKG_NAME");
    let about = "A git special remote that permits using a git branch as an upstream for one or more repositories.";
    let authors = env!("CARGO_PKG_AUTHORS");
    let version = format!(
        "{} ({})",
        env!("CARGO_PKG_VERSION"),
        BUILD_STAMP.git_revision_cleanness()
    );

    let mut app = App::new("git-remote-recursive")
        .name(pkg_name)
        .about(about)
        .version(&*version)
        .author(authors)
        .arg_from_usage("-c, --configuration 'Prints configuration information/examples.'")
        .arg_from_usage("-g, --generate-configuration 'Prints an example config for embedding.'")
        .arg_from_usage("-d, --debug 'Dumps tracking repository state.'")
        .arg_from_usage("-e, --embed-configuration=[config] 'Encodes the recursive remote options under the [remote] section in git config file [config] into a format that can be used in place of the remote spec for git clone, etc. Use -g for an example.'")
        .arg_from_usage("-p, --parse-configuration=[config] 'Parses the encoded configuration [config] and prints the corresponding git config.'")
        .arg_from_usage("[remote_name_passed_from_git]")
        .arg_from_usage("[remote_spec_passed_from_git]");

    let matches = app.clone().get_matches();

    if matches.contains_id("configuration") {
        recursive_remote::config::print_configuration_guidance();
        Ok(())
    } else if matches.contains_id("embed-configuration") {
        let path = matches
            .get_one::<String>("embed-configuration")
            .expect("Logic error");
        let entries = recursive_remote::embedded_config::embed_file(&PathBuf::from(path))
            .with_context(|| format!("embed git config file {}", &path))?;
        for (embedded, url) in entries.into_values() {
            let url = url.unwrap_or_default();
            let url = if url.starts_with("recursive::") {
                &url[11..]
            } else {
                &url[..]
            };
            println!("recursive::{}:{}", &embedded, &url);
        }
        Ok(())
    } else if matches.contains_id("parse-configuration") {
        let embedded = matches
            .get_one::<String>("parse-configuration")
            .expect("Logic error");
        let mut embedded = &embedded[..];
        if embedded.starts_with("recursive::") {
            embedded = &embedded[11..];
        }
        let tok: Vec<_> = embedded.splitn(2, ':').collect();
        if tok.len() == 2 && embedded.starts_with("0") {
            match recursive_remote::embedded_config::parse(tok[0], "remote_name") {
                Ok(parsed) => {
                    println!("{}", &parsed);
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("Unable to parse embedded configuration, but heuristics indicate that doing so may be intended: {}", e);
                }
            }
        }
        eprintln!("No valid embedded config was specified.");
        Ok(())
    } else if matches.contains_id("generate-configuration") {
        println!("[remote]");
        println!("\trecursive-remote-branch = main");
        println!("\trecursive-namespace = my_repo");
        println!("\trecursive-namespace-nacl-key = file://.creds/nacl_namespace");
        println!("\trecursive-state-nacl-key = file://.creds/nacl_state");
        Ok(())
    } else {
        match (
            matches.get_one::<String>("remote_name_passed_from_git"),
            matches.get_one::<String>("remote_spec_passed_from_git"),
        ) {
            (Some(remote_name), Some(remote_spec)) => git_special_remote_main(
                remote_name.as_ref(),
                remote_spec.as_ref(),
                matches.contains_id("debug"),
            ),
            _ => {
                app.print_help().ok();
                std::process::exit(1);
            }
        }
    }
}

fn do_debug_dump(config: &Config) -> Result<()> {
    let tracking_repo = Rc::new(config.tracking_repo().context("open tracking repo")?);
    let (_commit_oid, (state_identifier, state), _root_oid) =
        match resolve_state_ref(&tracking_repo, &config.nacl_keys, &config.tracking_ref)
            .context("get state oid for tracking ref")?
        {
            Some(stuff) => stuff,
            None => {
                anyhow::bail!("No current commit on tracking branch.");
            }
        };

    eprint!("State: {}\n\tParents:", &state_identifier);
    for parent in state.parents.iter() {
        eprint!(" {}", &parent);
    }
    eprintln!("\n\tNamespaces:");
    for (name, namespace) in state.namespaces.iter() {
        eprintln!("\t\t{} -> {}", &name, &namespace);
    }
    eprintln!("\n");

    let mut ref_targets_by_namespace = std::collections::HashMap::new();
    for name in state.namespaces.keys() {
        eprintln!("Namespace {}:", name);
        let ns = state
            .namespace(name, &config.nacl_keys, &tracking_repo)
            .with_context(|| format!("decode namespace {}", name))?
            .expect("");
        match ns.pack.as_ref() {
            None => eprintln!("\t<no pack>"),
            Some(pack) => eprintln!("\tPack: {}", &pack),
        }
        eprintln!("\tRefs:");
        let mut ref_targets = std::collections::HashSet::new();
        for (name, target) in ns.refs.iter() {
            eprintln!("\t\t{} -> {}", name, target);
            if let Some(oid) = target.oid_at_time() {
                ref_targets.insert(oid.to_string());
            }
        }
        ref_targets_by_namespace.insert(name.to_string(), ref_targets);
    }
    eprintln!("\n");

    for name in state.namespaces.keys() {
        eprintln!("History for Namespace {}", name);
        let ordered_packs = recursive_remote::cmd_fetch::materialize_ordered_pack_list(
            &config,
            &tracking_repo,
            Some(&state_identifier),
            &state,
            None,
        )?;
        let mut commits_in_pack = std::collections::HashMap::new();
        let mut all_commits = std::collections::HashSet::new();
        for pack_name in ordered_packs {
            eprintln!("\t{}", &pack_name);
            match recursive_remote::cmd_fetch::fetch_pack(
                config,
                &tracking_repo,
                pack_name.clone(),
            )? {
                Some(git_pack_name) => {
                    let git_pack_name = hex::encode(git_pack_name);
                    eprintln!("\t\t{}", &git_pack_name);

                    let mut commits = std::collections::HashSet::new();

                    let mut cmd = recursive_remote::util::git_command()
                        .current_dir(&config.all_objects_ever_repo_path)
                        .arg("verify-pack")
                        .arg("--verbose")
                        .arg(format!(
                            "{}/objects/pack/pack-{}.idx",
                            config.all_objects_ever_repo_path.display(),
                            &git_pack_name
                        ))
                        .stdin(std::process::Stdio::null())
                        .stdout(std::process::Stdio::piped())
                        .stderr(std::process::Stdio::piped())
                        .spawn()
                        .context("Failed to spawn git pack-objects.")?;

                    let stdout = cmd.stdout.take().context("No stdout.")?;
                    for line in std::io::BufReader::new(stdout).lines() {
                        let line = line?;
                        if line.contains(" commit ") {
                            eprintln!("\t\t\t{}", &line);
                            if let Some(id) = line.split_ascii_whitespace().next() {
                                commits.insert(id.to_string());
                                all_commits.insert(id.to_string());
                            }
                        }
                    }
                    commits_in_pack.insert(pack_name.clone(), commits);
                }
                None => {
                    eprintln!("\t\t<empty pack>");
                }
            }
        }

        for (namespace, oids) in ref_targets_by_namespace.iter() {
            for oid in oids.iter() {
                if !all_commits.contains(oid) {
                    eprintln!("Namespace {namespace} missing commit {oid}");
                }
            }
        }

        eprintln!("\n");
    }

    // let repo_path = matches.get_one::<PathBuf>("remote_repo_path").context("No path to a repo specified to dump.")?;
    // let tracking_repo = Rc::new(git2::Repository::open_bare(&repo_path).context("open bare git tracking repo")?);

    // // Just brute force for dumping namespace content.
    // let mut keys: Vec<_> = match matches.get_one::<String>("key_file_first_line_is_state_rest_namespace") {
    //     Some(key_file) => {
    //         let fd = std::fs::File::open(&key_file)?;
    //         let fd = std::io::BufReader::new(fd);
    //         let mut keys = Vec::default();
    //         for line in fd.lines() {
    //             let line = line?;
    //             keys.push(eseb::SymmetricKey::from_str(&line).context("parse key")?);
    //         }
    //         let state_key = &keys[0];
    //         keys.drain(1..).map(|namespace_key| {
    //             let state_key=state_key.clone();
    //             let inner = EncryptionKeysInner {state_key, namespace_key};
    //             EncryptionKeys{inner: Some(inner)}
    //         }
    //         ).collect()
    //     }
    //     None => Vec::default(),
    // };
    // keys.push(EncryptionKeys{inner: None});

    // dump_state(&tracking_repo, &keys, &config.tracking_ref)

    // for key in keys.iter() {
    //     let cur_oid = resolve_state_ref(&tracking_repo, &config.nacl_keys, &config.tracking_ref).context("get state oid for tracking ref")?;
    // }

    // crate::encoding::decode_state(
    //     &tracking_repo,
    //     &state_identifier,
    //     &config.nacl_keys,
    // )

    Ok(())
}

fn git_special_remote_main(remote_name: &str, remote_spec: &str, debug_dump: bool) -> Result<()> {
    let args = Args::new(remote_name, remote_spec).context("parse Args")?;

    std::fs::create_dir_all(&args.state_path).context("create state repo dir")?;
    std::fs::create_dir_all(&args.lock_path).context("create locks dir")?;

    let (config, _branch_lock) = {
        let state_repo_lock =
            recursive_remote::util::acquire_flock(&args.lock_path.join("recursive_remote.lock"))
                .context("Failed to lock state repo lock file.")?;
        let config = initialize_state_repo(args)?;
        let lock = recursive_remote::util::acquire_flock(
            &config
                .lock_path
                .join(&format!("recursive_remote.{}.lock", &config.remote_name)),
        );

        // My initial intent was to have per-remote locking. I've decided to
        // instead go with a global lock, as I see no real advantage to
        // per-remote locking and it makes managing our state riskier.

        (config, (state_repo_lock, lock))
    };

    if debug_dump {
        return do_debug_dump(&config);
    }

    let lines = std::io::stdin();
    let mut lines = lines.lock().lines();

    loop {
        match lines.next() {
            Some(Ok(line)) => {
                trace!("Received command: {:?}", &line);

                let mut tok = line.split_ascii_whitespace();
                let command = tok.next().unwrap_or_default();

                if command == "capabilities" {
                    println!("push\nfetch\n");
                } else if command.starts_with("list") {
                    let (_, state, _basis_state, _root_id, _commit_id) = update_branches(&config)?;
                    let namespace = state
                        .namespace(
                            &config.namespace,
                            &config.nacl_keys,
                            &Rc::new(config.tracking_repo()?),
                        )?
                        .unwrap_or_else(Namespace::new);
                    for (name, target) in namespace.refs.iter() {
                        trace!("\t{} -> {}", &name, &target.to_git_wire_string());
                        println!("{} {}", &target.to_git_wire_string(), &name);
                    }
                    println!("");
                } else if command.starts_with("push") {
                    let pushes =
                        collect_lines(&mut lines, "push", Some(line)).context("push collect")?;
                    recursive_remote::cmd_push::push(&config, &pushes)
                        .context("Failed to push.")?;
                } else if command.starts_with("fetch") {
                    let fetches =
                        collect_lines(&mut lines, "fetch", Some(line)).context("fetch collect")?;
                    recursive_remote::cmd_fetch::fetch(&config, &fetches)
                        .context("Failed to fetch.")?;
                }
            }
            None => break,
            Some(Err(e)) => panic!("Error: {:?}", &e),
        }
    }

    Ok(())
}
