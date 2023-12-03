use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use eseb::KeyMaterial;
use log::{info, trace};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::embedded_config::*;
use crate::serialization::Ref;
use crate::util::*;

#[derive(
    Copy, EnumIter, serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone, Hash,
)]
pub enum ConfigKey {
    Namespace,
    RemoteBranch,
    NamespaceNaclKey,
    StateNaclKey,
    ShallowBasis,
    MaxObjectSize,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum ConfigValue {
    String(String),
    Int64(i64),
}

pub struct Args {
    pub user_repo_path: PathBuf,
    pub tracking_repo_path: PathBuf,
    pub push_semantics_repo_path: PathBuf,
    pub all_objects_ever_repo_path: PathBuf,
    pub remote_name: String,
    pub lock_path: PathBuf,
    pub state_path: PathBuf,
    pub remote_url: String,
}

pub struct EncryptionKeys {
    pub inner: Option<EncryptionKeysInner>,
}

pub struct EncryptionKeysInner {
    pub state_key: eseb::SymmetricKey,
    pub namespace_key: eseb::SymmetricKey,
}

impl std::fmt::Display for ConfigKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl AsRef<str> for ConfigKey {
    fn as_ref(&self) -> &'static str {
        match self {
            ConfigKey::Namespace => "recursive-namespace",
            ConfigKey::RemoteBranch => "recursive-remote-branch",
            ConfigKey::NamespaceNaclKey => "recursive-namespace-nacl-key",
            ConfigKey::StateNaclKey => "recursive-state-nacl-key",
            ConfigKey::ShallowBasis => "recursive-shallow-basis",
            ConfigKey::MaxObjectSize => "recursive-max-object-size",
        }
    }
}

impl ConfigKey {
    pub fn is_i64(&self) -> bool {
        match self {
            ConfigKey::Namespace => false,
            ConfigKey::RemoteBranch => false,
            ConfigKey::NamespaceNaclKey => false,
            ConfigKey::StateNaclKey => false,
            ConfigKey::ShallowBasis => false,
            ConfigKey::MaxObjectSize => true,
        }
    }

    pub fn as_short_str(&self) -> &'static str {
        match self {
            ConfigKey::Namespace => "a",
            ConfigKey::RemoteBranch => "b",
            ConfigKey::NamespaceNaclKey => "c",
            ConfigKey::StateNaclKey => "d",
            ConfigKey::ShallowBasis => "e",
            ConfigKey::MaxObjectSize => "f",
        }
    }

    pub fn from_short_str(short_str: &str) -> Option<ConfigKey> {
        match short_str {
            "a" => Some(ConfigKey::Namespace),
            "b" => Some(ConfigKey::RemoteBranch),
            "c" => Some(ConfigKey::NamespaceNaclKey),
            "d" => Some(ConfigKey::StateNaclKey),
            "e" => Some(ConfigKey::ShallowBasis),
            "f" => Some(ConfigKey::MaxObjectSize),
            _ => None,
        }
    }
}

impl std::fmt::Display for ConfigValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigValue::Int64(v) => write!(f, "i64[{}]", v),
            ConfigValue::String(v) => write!(f, "str[{}]", v),
        }
    }
}

impl EncryptionKeys {
    // Used to encrypt state.bincode, which describes the namespaces and the sha256
    // history. Must be the same for all namespaces stored on this branch of
    // upstream.
    pub fn state_key(&self) -> Option<&eseb::SymmetricKey> {
        match self.inner.as_ref() {
            Some(keys) => Some(&keys.state_key),
            None => None,
        }
    }

    // Used to encrypt namespace.bincode and the pack files. May be different for
    // each namespace, but need not be. You can also use the same key for state
    // and namespace for the simple case where access to any namespace on the
    // branch and all are equivalent.
    pub fn namespace_key(&self) -> Option<&eseb::SymmetricKey> {
        match self.inner.as_ref() {
            Some(keys) => Some(&keys.namespace_key),
            None => None,
        }
    }
}

pub struct Config {
    // The namespace to operate on. Each remote branch can support multiple
    // namespaces, each a separate repo with its own encryption key.
    pub namespace: String,

    // The user's git repo path.
    pub user_repo_path: PathBuf,

    // Our tracking repo path.
    pub tracking_repo_path: PathBuf,

    // The name of the remote we are in the user repo, and also the name of the
    // tracking remote in the tracking repo.
    pub remote_name: String,
    pub tracking_ref: String,
    pub all_objects_ever_repo_path: PathBuf,
    pub push_semantics_repo_path: PathBuf,
    pub state_path: PathBuf,
    pub lock_path: PathBuf,
    pub pushing_ref: String,
    pub basis_ref: String,
    pub remote_url: String,
    pub remote_ref: String,
    pub nacl_keys: EncryptionKeys,
    pub shallow_basis: Vec<Ref>,
    pub max_object_size: usize,
}

impl Args {
    pub fn new(remote_name: &str, remote_url: &str) -> Result<Args> {
        let user_repo_path = std::fs::canonicalize(
            std::env::var("GIT_DIR")
                .context("Git dir not defined.")?
                .to_string(),
        )?;

        let state_path = user_repo_path.join("recursive_remote");
        let tracking_repo_path = state_path.join("tracking_repo");
        let push_semantics_repo_path = state_path.join("push_semantics_repo");
        let all_objects_ever_repo_path = state_path.join("all_objects_ever_repo");
        let lock_path = state_path.join("locks");

        Ok(Args {
            user_repo_path,
            lock_path,
            tracking_repo_path,
            push_semantics_repo_path,
            all_objects_ever_repo_path,
            state_path,
            remote_name: remote_name.to_string(),
            remote_url: remote_url.to_string(),
        })
    }

    pub fn user_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.user_repo_path).context("open user repo.")
    }

    pub fn tracking_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.tracking_repo_path).context("open user repo.")
    }

    pub fn push_semantics_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.push_semantics_repo_path)
            .context("open push semantics repo.")
    }

    pub fn all_objects_ever_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.all_objects_ever_repo_path)
            .context("open all_objects_ever repo.")
    }
}

impl Config {
    pub fn new(mut args: Args) -> Result<Config> {
        let tracking_repo = args.tracking_repo()?;
        let user_repo = args.user_repo()?;

        let mut tracking_config = tracking_repo.config().ok().context("tracking config")?;
        let mut user_config = user_repo
            .config()
            .ok()
            .context("user config")?
            .snapshot()
            .context("snapshot")?;

        let tok: Vec<_> = args.remote_url.splitn(2, ':').collect();
        let (remote_url, _tmp) = if tok.len() != 2 || !tok[0].starts_with("0") {
            (args.remote_url.as_str(), None)
        } else {
            let tmp =
                tempdir::TempDir::new("recursive_remote").context("Unable to create temp dir.")?;
            let config_path = tmp.path().join("git_config");
            match crate::embedded_config::parse_into_file(tok[0], &args.remote_name, &config_path) {
                Ok(..) => {
                    user_config.add_file(
                        &config_path,
                        git2::ConfigLevel::App,
                        /*force=*/ false,
                    )?;
                    (tok[1], Some(tmp))
                }
                Err(..) => {
                    log::warn!("Unable to parse URL \"{}\" for embedded configuration, but heuristics indicate that doing so may be intended.", &args.remote_url);
                    (args.remote_url.as_str(), None)
                }
            }
        };

        args.remote_url = remote_url.to_string();

        let namespace = configure_namespace(&args, &user_config).context("namespace config")?;
        let shallow_basis =
            configure_shallow_basis(&user_repo, &args, &user_config).context("shallow basis")?;

        let max_object_size = read_config_i64(&args, ConfigKey::MaxObjectSize, &user_config)
            .context("max object size")?
            .unwrap_or(20 * 1024 * 1024);

        let max_object_size: usize = max_object_size
            .try_into()
            .context("max_object_size must be >= 10")?;
        if max_object_size < 10 {
            anyhow::bail!("max_object_size must be >= 10");
        }

        // Don't want to deal with 32 vs 64 problems in encoding/rio.
        if max_object_size > 1024 * 1024 * 1024 {
            anyhow::bail!("max_object_size must be <= 1024 * 1024 * 1024");
        }

        clean_tracking_config(&args, &mut tracking_config).context("tracking config clean")?;
        configure_tracking_config(&args, &user_config, &mut tracking_config)
            .context("configure tracking config")?;

        let remote_ref =
            configure_remote_branch(&args, &user_config).context("remote branch config")?;

        let nacl_keys = {
            let mut mutable_user_config = user_repo.config().ok().context("mutable user config")?;
            let namespace_key =
                configure_nacl(ConfigKey::NamespaceNaclKey, &args, &mut mutable_user_config)
                    .context("nacl namespace key config")?;
            let state_key =
                configure_nacl(ConfigKey::StateNaclKey, &args, &mut mutable_user_config)
                    .context("nacl state key config")?;
            match (namespace_key, state_key) {
                (Some(namespace_key), Some(state_key)) => Some(EncryptionKeysInner {
                    namespace_key,
                    state_key,
                }),
                (None, None) => None,
                _ => panic!(
                    "Both or neither of namespace-nacl-key and state-nacl-key must be provided."
                ),
            }
        };
        let nacl_keys = EncryptionKeys { inner: nacl_keys };

        let tracking_ref = format!("refs/heads/{}/tracking", &args.remote_name);
        let pushing_ref = format!("refs/heads/{}/push", &args.remote_name);
        let basis_ref = if namespace.is_empty() {
            format!("refs/heads/{}/default_basis", &args.remote_name)
        } else {
            format!("refs/heads/{}/basis/{}", &args.remote_name, &namespace)
        };

        Ok(Config {
            namespace,
            user_repo_path: args.user_repo_path,
            tracking_repo_path: args.tracking_repo_path,
            state_path: args.state_path,
            push_semantics_repo_path: args.push_semantics_repo_path,
            all_objects_ever_repo_path: args.all_objects_ever_repo_path,
            remote_name: args.remote_name,
            remote_url: args.remote_url,
            lock_path: args.lock_path,
            remote_ref,
            tracking_ref,
            pushing_ref,
            basis_ref,
            nacl_keys,
            shallow_basis,
            max_object_size,
        })
    }

    pub fn all_objects_ever_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.all_objects_ever_repo_path)
            .context("open all_objects_ever repo.")
    }

    pub fn user_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.user_repo_path).context("open user repo.")
    }

    pub fn tracking_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.tracking_repo_path).context("open tracking repo.")
    }

    pub fn push_semantics_repo(&self) -> Result<git2::Repository> {
        open_create_bare_repository(&self.push_semantics_repo_path)
            .context("open push semantics repo.")
    }
}

fn configure_namespace(args: &Args, git_config: &git2::Config) -> Result<String> {
    match read_config(args, ConfigKey::Namespace, &git_config)? {
        Some(namespace) => Ok(namespace),
        None => Ok(String::default()),
    }
}

fn configure_nacl_key(
    c_key: ConfigKey,
    value: String,
    args: &Args,
    git_config: &mut git2::Config,
) -> Result<Option<eseb::SymmetricKey>> {
    let key = if value.is_empty() {
        info!(
            "Storing newly created {} NaCl key directly in git config.",
            c_key
        );
        let key = eseb::SymmetricKey::gen_key().context("gen key")?;
        write_config(
            &args.remote_name,
            c_key,
            git_config,
            &key.serialize_to_string(),
        )
        .context("write_config")?;
        key
    } else {
        eseb::SymmetricKey::from_str(&value).context("parse key")?
    };
    Ok(Some(key))
}

fn configure_nacl_key_file(value: &str) -> Result<Option<eseb::SymmetricKey>> {
    let key = match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&value)
    {
        Ok(mut fd) => {
            info!("Storing newly created NaCl key in file {:?}.", &value);
            let key = eseb::SymmetricKey::gen_key().context("gen key file")?;
            fd.write_all(&key.serialize_to_string().as_bytes())?;
            key
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            trace!("Reading key file: {:?}", &value);
            let mut fd = std::fs::File::open(&value)?;
            let mut s = String::default();
            fd.read_to_string(&mut s).context("Failed to read key.")?;
            eseb::SymmetricKey::from_str(s.trim()).context("decode key")?
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && value.starts_with("~/") => {
            let home = std::env::var("HOME").context("read env var HOME")?;
            let value = PathBuf::from(home).join(&value[2..]);
            return configure_nacl_key_file(&*value.to_string_lossy());
        }
        Err(e) => return Err(e.into()),
    };

    Ok(Some(key))
}

pub fn configure_nacl(
    c_key: ConfigKey,
    args: &Args,
    git_config: &mut git2::Config,
) -> Result<Option<eseb::SymmetricKey>> {
    match read_config(args, c_key, &git_config)? {
        None => Ok(None),
        Some(value) if value.starts_with("file://") => configure_nacl_key_file(&value[7..]),
        Some(value) => configure_nacl_key(c_key, value, args, git_config),
    }
}

pub fn configure_remote_branch(args: &Args, git_config: &git2::Config) -> Result<String> {
    Ok(
        match read_config(&args, ConfigKey::RemoteBranch, &git_config)? {
            Some(branch) => {
                if branch.starts_with("refs/heads/") {
                    branch
                } else {
                    format!("refs/heads/{}", &branch)
                }
            }
            None => String::from("refs/heads/main"),
        },
    )
}

pub fn configure_shallow_basis(
    user_repo: &git2::Repository,
    args: &Args,
    git_config: &git2::Config,
) -> Result<Vec<Ref>> {
    let mut refs = Vec::default();
    for spec in read_config(&args, ConfigKey::ShallowBasis, &git_config)?
        .unwrap_or_default()
        .split_whitespace()
    {
        refs.push(
            Ref::new(user_repo, &spec)
                .with_context(|| format!("resolve shallow basis ref {}", &spec))?,
        );
    }
    Ok(refs)
}

pub fn read_config(
    args: &Args,
    key: ConfigKey,
    git_config: &git2::Config,
) -> Result<Option<String>> {
    match git_config.get_string(&format!("remote.{}.{}", &args.remote_name, key)) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.code() == git2::ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e).context(key),
    }
}

pub fn read_config_i64(
    args: &Args,
    key: ConfigKey,
    git_config: &git2::Config,
) -> Result<Option<i64>> {
    match git_config.get_i64(&format!("remote.{}.{}", &args.remote_name, key)) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.code() == git2::ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e).context(key),
    }
}

pub fn write_config(
    remote_name: &str,
    key: ConfigKey,
    git_config: &mut git2::Config,
    value: &str,
) -> Result<()> {
    git_config
        .set_str(&format!("remote.{}.{}", remote_name, key), value)
        .context(key)
}

pub fn write_config_i64(
    remote_name: &str,
    key: ConfigKey,
    git_config: &mut git2::Config,
    value: i64,
) -> Result<()> {
    git_config
        .set_i64(&format!("remote.{}.{}", remote_name, key), value)
        .context(key)
}

pub fn clean_tracking_config(args: &Args, tracking_config: &mut git2::Config) -> Result<()> {
    let mut old_tracking_config = Vec::new();
    {
        let mut it = tracking_config
            .entries(Some(&format!("remote.{}.*", &args.remote_name)))
            .context("list recursion tracking config")?;
        while let Some(entry) = it.next() {
            let entry = entry.context("entry")?;
            let name = entry.name().context("non-utf8 key")?;
            old_tracking_config.push(name.to_string());
        }
    }

    for entry in old_tracking_config {
        tracking_config
            .remove(&entry)
            .context("remove config entry")?;
    }

    Ok(())
}

pub fn configure_tracking_config(
    args: &Args,
    user_config: &git2::Config,
    tracking_config: &mut git2::Config,
) -> Result<()> {
    tracking_config
        .set_str(
            &format!("remote.{}.url", &args.remote_name),
            &args.remote_url,
        )
        .context("set remote url")?;

    let prefix = format!("remote.{}.recursion-inner-*", &args.remote_name);

    let mut it = user_config
        .entries(Some(&prefix))
        .context("list recursion inner config")?;
    while let Some(entry) = it.next() {
        let entry = entry.context("entry")?;
        let name = entry.name().context("non-utf8 key")?;
        let value = user_config.get_str(&name).context("get config value")?;
        let s_key = format!(
            "remote.{}.{}",
            &args.remote_name,
            &name[prefix.as_bytes().len() - 1..]
        );
        tracking_config
            .set_str(&s_key, &value)
            .context("set tracking config")?;
    }

    Ok(())
}

pub fn print_key_configuration_guidance(key: ConfigKey) {
    match key {
        ConfigKey::Namespace => {
            println!("\trecursive-namespace: Each branch on the remote repository can have multiple namespaces, each acting as an upstream for a separate repository. Unset is the same as empty string.");
        }
        ConfigKey::RemoteBranch => {
            println!("\trecursive-remote-branch: The branch on the remote repository to use. Defaults to 'main'.");
        }
        ConfigKey::NamespaceNaclKey => {
            println!("\trecursive-namespace-nacl-key: The encryption key to use to encrypt this repository's contents on the remote.");
        }
        ConfigKey::StateNaclKey => {
            println!("\trecursive-state-nacl-key: The encryption key to use to encrypt the branch metadata. All namespaces (repositories) on the same remote branch must use the same key.");
        }
        ConfigKey::ShallowBasis => {
            println!("\trecursive-shallow-basis: Space-separated list of refs that don't need to be stored upstream. This is somewhat analogous to git shallow clone, though it is the upstream that is shallow instead of the local repository. This can be used to synchronize a repository across several machines that share large common history without needing to store the entire history upstream, but any new clones will need to get that common history via another mechanism such as an existing remote.");
        }
        ConfigKey::MaxObjectSize => {
            println!("\trecursive-max-object-size: Attempt to split objects stored upstream into chunks around this size.");
        }
    }
}

pub fn print_configuration_guidance() {
    eprintln!("# Default namespace, generate encryption keys on first use.");
    eprintln!("\t[remote \"origin\"]");
    eprintln!("\t\turl = recursive::file:///home/username/recursive-upstream-repo");
    eprintln!("\t\tfetch = +refs/heads/*:refs/remotes/origin/*");
    eprintln!("\t\trecursive-remote-branch = main");
    eprintln!("\t\trecursive-namespace = \"\"");
    eprintln!("\t\trecursive-namespace-nacl-key = \"\"");
    eprintln!("\t\trecursive-state-nacl-key = \"\"");
    eprintln!();

    eprintln!("# Namespace work, branch org, unencrypted");
    eprintln!("\t[remote \"origin\"]");
    eprintln!("\t\turl = recursive::git@github.com:username/orgrepo.git");
    eprintln!("\t\tfetch = +refs/heads/*:refs/remotes/origin/*");
    eprintln!("\t\trecursive-remote-branch = org");
    eprintln!("\t\trecursive-namespace = work");
    eprintln!();

    eprintln!("# Default namespace, use same key file for state and namespace");
    eprintln!("# (generates keys on first use if file does not exist)");
    eprintln!("\t[remote \"origin\"]");
    eprintln!("\t\turl = recursive::file:///home/username/recursive-upstream-repo");
    eprintln!("\t\tfetch = +refs/heads/*:refs/remotes/origin/*");
    eprintln!("\t\trecursive-remote-branch = main");
    eprintln!("\t\trecursive-namespace = \"\"");
    eprintln!("\t\trecursive-namespace-nacl-key = \"file://.creds/recursive_remote_key\"");
    eprintln!("\t\trecursive-state-nacl-key = \"file://.creds/recursive_remote_key\"");
    eprintln!();

    eprintln!("The following configuration keys are available:");
    for key in ConfigKey::iter() {
        print_key_configuration_guidance(key);
    }
}
