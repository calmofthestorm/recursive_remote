use std::collections::HashMap;
use std::collections::HashSet;
use std::io::Read as _;
use std::path::Path;

use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use strum::IntoEnumIterator;

use crate::config::*;

/// Allows specifying config overrides as an encoded string in the url. The main
/// use case here is when we are using a program that builds on git (e.g.,
/// Cargo), where we cannot set config options on the repo, as well as to enable
/// clone with a one-liner.
///
/// These override the values obtained from git config if present.
#[derive(Default, serde::Serialize, serde::Deserialize, PartialEq, Eq, Debug, Clone)]
struct EmbeddedConfig(HashMap<String, ConfigValue>);

pub fn embed(config: &git2::Config) -> Result<HashMap<String, (String, Option<String>)>> {
    let mut remotes = HashSet::new();

    let mut entries = config.entries(Some("remote*")).unwrap();
    while let Some(entry) = entries.next() {
        if let Ok(Some(name)) = entry.map(|e| e.name()) {
            if let Some(name) = name.split('.').nth(1) {
                remotes.insert(name.to_string());
            }
        }
    }

    let mut embeds = HashMap::new();
    for remote in remotes {
        let embed =
            embed_remote(config, &remote).with_context(|| format!("embed remote {}", &remote))?;
        embeds.insert(remote, embed);
    }

    Ok(embeds)
}

pub fn embed_remote(config: &git2::Config, remote: &str) -> Result<(String, Option<String>)> {
    let mut map = HashMap::new();

    for key in ConfigKey::iter() {
        let remote_key = format!("remote.{}.{}", remote, key);
        let v = if key.is_i64() {
            config.get_i64(&remote_key).map(ConfigValue::Int64)
        } else {
            config.get_string(&remote_key).map(ConfigValue::String)
        };

        match v {
            Ok(v) => {
                map.insert(key.as_short_str().to_string(), v);
            }
            Err(e) if e.code() == git2::ErrorCode::NotFound => {}
            Err(e) => return Err(e).context(remote_key),
        }
    }

    let remote_key = format!("remote.{}.url", remote);
    let url = match config.get_string(&remote_key) {
        Ok(url) => Some(url),
        Err(e) if e.code() == git2::ErrorCode::NotFound => None,
        Err(e) => return Err(e).context(remote_key),
    };

    let map = EmbeddedConfig(map);

    let mut compressor = brotli::CompressorWriter::new(
        Vec::default(),
        /*buffer_size=*/ 8192,
        /*compression_quality=*/ 11,
        /*lgwin=*/ 24,
    );

    bincode::serialize_into(&mut compressor, &map)
        .context("bincode serialize embedded config map")?;

    let map = compressor.into_inner();
    let map: String = format!("0{}", URL_SAFE_NO_PAD.encode(&map));

    Ok((map, url))
}

pub fn embed_file(path: &Path) -> Result<HashMap<String, (String, Option<String>)>> {
    let config = git2::Config::open(path).context("parse git config file")?;
    embed(&config).context("embed config")
}

pub fn parse(embedded: &str, remote_name: &str) -> Result<String> {
    let tmp = tempdir::TempDir::new("recursive_remote")?;
    let tmp = tmp.path().join("git_config");
    parse_into_file(embedded, remote_name, &tmp)?;
    std::fs::read_to_string(&tmp)
        .with_context(|| format!("read temp git config at {}", tmp.display()))
}

pub fn parse_into_file(embedded: &str, remote_name: &str, out: &Path) -> Result<()> {
    if !embedded.starts_with("0") {
        anyhow::bail!("unknown version or invalid");
    }
    let embedded = URL_SAFE_NO_PAD.decode(&embedded[1..]).context("base64")?;

    let decompressor = brotli::reader::Decompressor::new(embedded.as_slice(), 8192);

    let map =
        bincode::deserialize_from::<_, EmbeddedConfig>(decompressor).context("deserialize")?;

    let mut config = git2::Config::open(out).context("create empty git config on disk")?;
    for (key, value) in map.0.iter() {
        if let Some(key) = ConfigKey::from_short_str(key) {
            let remote_key = format!("remote.{}.{}", remote_name, &key);
            match value {
                ConfigValue::Int64(v) => config.set_i64(&remote_key, *v),
                ConfigValue::String(v) => config.set_str(&remote_key, v),
            }
            .with_context(|| format!("set git config key {} to {}", &remote_key, &value))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &'static str = r#"
[remote "qux"]
    recursive-namespace = foo
    recursive-remote-branch = "bar-baz"
    recursive-max-object-size = 1048576
    unknown-are-ignored = 123

[unknownsection]
    chicken = "cluck"
"#;

    #[test]
    fn test_embedded_config() {
        let remote_name = "myremote";
        let tmp = tempdir::TempDir::new("rust-test").unwrap();
        let tmp = tmp.path();
        let config_path = tmp.join("git_config");
        let parsed_config_path = tmp.join("parsed_git_config");

        std::fs::write(&config_path, &TEXT).unwrap();

        let embedded = embed_file(&config_path).unwrap();
        assert_eq!(embedded.len(), 1);
        let (embedded, _url) = embedded.get("qux").unwrap();
        let parsed = parse(&embedded, remote_name).unwrap();
        std::fs::write(&parsed_config_path, &parsed).unwrap();

        let config = git2::Config::open(&parsed_config_path).unwrap();

        let keygen = |key| format!("remote.{}.{}", remote_name, &key);

        assert_eq!(
            config.get_i64(&keygen(ConfigKey::MaxObjectSize)).unwrap(),
            1048576
        );
        assert_eq!(
            config.get_string(&keygen(ConfigKey::Namespace)).unwrap(),
            "foo"
        );
        assert_eq!(
            config.get_string(&keygen(ConfigKey::RemoteBranch)).unwrap(),
            "bar-baz"
        );
    }

    #[test]
    fn test_git_overrides() {
        let tmp = tempdir::TempDir::new("rust-test").unwrap();
        let tmp = tmp.path();
        let config1_path = tmp.join("git_config1");
        let config2_path = tmp.join("git_config2");

        {
            let mut config = git2::Config::open(&config1_path).unwrap();
            config.set_str("remote.foo", "bees").unwrap();
            config.set_str("remote.bar", "wasps").unwrap();
        }

        {
            let mut config = git2::Config::open(&config2_path).unwrap();
            config.set_str("remote.qux", "hornets").unwrap();
            config.set_str("remote.bar", "fire ants").unwrap();
        }

        let mut config = git2::Config::new().unwrap();
        config
            .add_file(&config1_path, git2::ConfigLevel::Local, true)
            .unwrap();
        config
            .add_file(&config2_path, git2::ConfigLevel::App, true)
            .unwrap();
        assert_eq!("bees", &config.get_string("remote.foo").unwrap());
        assert_eq!("fire ants", &config.get_string("remote.bar").unwrap());
        assert_eq!("hornets", &config.get_string("remote.qux").unwrap());
    }
}
