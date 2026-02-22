use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use gix::diff::object::bstr::BStr;
use gix_config::Source;
use gix_sec::Trust;
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

pub fn embed(config: &gix_config::File) -> Result<HashMap<String, (String, Option<String>)>> {
    let mut remotes = HashSet::new();

    if let Some(sections) = config.sections_by_name("remote") {
        for section in sections {
            if let Some(name) = section.header().subsection_name() {
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

pub fn embed_remote(config: &gix_config::File, remote: &str) -> Result<(String, Option<String>)> {
    let mut map = HashMap::new();
    let subsection: &BStr = remote.as_bytes().into();

    for key in ConfigKey::iter() {
        if let Some(v) = if key.is_i64() {
            config
                .integer_by("remote", Some(subsection), key)
                .transpose()?
                .map(ConfigValue::Int64)
        } else {
            config
                .string_by("remote", Some(subsection), key)
                .map(|v| ConfigValue::String(Cow::Owned(v.to_string())))
        } {
            map.insert(key.as_short_str().to_string(), v);
        }
    }

    let url = config
        .string_by("remote", Some(subsection), "url")
        .map(|s| s.to_string());
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
    let config = gix_config::File::from_paths_metadata(
        Some(gix_config::file::Metadata {
            path: Some(path.into()),
            source: Source::Api,
            level: 0,
            trust: Trust::Full,
        }),
        gix_config::file::init::Options::default(),
    )?
    .context("parse git config file")?;
    embed(&config).context("embed config")
}

pub fn parse(embedded: &str, remote_name: &str) -> Result<String> {
    let tmp = tempfile::Builder::new()
        .prefix("recursive_remote")
        .tempdir()?;
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

    let mut config = gix_config::File::new(gix_config::file::Metadata {
        path: Some(out.to_path_buf()),
        source: Source::Local,
        level: 0,
        trust: Trust::Full,
    });
    for (key, value) in map.0.iter() {
        if let Some(key) = ConfigKey::from_short_str(key) {
            let subsection: &BStr = remote_name.as_bytes().into();
            let raw_value = match value {
                ConfigValue::Int64(v) => v.to_string(),
                ConfigValue::String(v) => v.to_string(),
            };
            config.set_raw_value_by("remote", Some(subsection), key, raw_value.as_str())?;
        }
    }

    config.write_to(&mut std::fs::File::create(out)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::*;

    const TEXT: &str = r#"
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
        let tmp = tempfile::Builder::new()
            .prefix("rust-test")
            .tempdir()
            .unwrap();
        let tmp = tmp.path();
        let config_path = tmp.join("git_config");
        let parsed_config_path = tmp.join("parsed_git_config");

        std::fs::write(&config_path, TEXT).unwrap();

        let embedded = embed_file(&config_path).unwrap();
        assert_eq!(embedded.len(), 1);
        let (embedded, _url) = embedded.get("qux").unwrap();
        let parsed = parse(embedded, remote_name).unwrap();
        std::fs::write(&parsed_config_path, &parsed).unwrap();

        let config = gix_config::File::from_paths_metadata(
            Some(gix_config::file::Metadata {
                path: Some(parsed_config_path),
                source: Source::Api,
                level: 0,
                trust: Trust::Full,
            }),
            gix_config::file::init::Options::default(),
        )
        .unwrap()
        .unwrap();

        let subsection: &BStr = remote_name.as_bytes().into();

        assert_eq!(
            config
                .integer_by("remote", Some(subsection), ConfigKey::MaxObjectSize)
                .unwrap()
                .unwrap(),
            1048576
        );
        assert_eq!(
            *config
                .string_by("remote", Some(subsection), ConfigKey::Namespace)
                .unwrap(),
            "foo"
        );
        assert_eq!(
            *config
                .string_by("remote", Some(subsection), ConfigKey::RemoteBranch)
                .unwrap(),
            "bar-baz"
        );
    }

    #[test]
    fn test_git_overrides() {
        let tmp = tempfile::Builder::new()
            .prefix("rust-test")
            .tempdir()
            .unwrap();
        let tmp = tmp.path();
        let config1_path = tmp.join("git_config1");
        let config2_path = tmp.join("git_config2");

        {
            let mut config = gix_config::File::new(gix_config::file::Metadata {
                path: Some(config1_path.clone()),
                source: Source::Api,
                level: 0,
                trust: Trust::Full,
            });

            config.set_raw_value(&"remote.foo", "bees").unwrap();
            config.set_raw_value(&"remote.bar", "wasps").unwrap();

            config
                .write_to(&mut std::fs::File::create(&config1_path).unwrap())
                .unwrap();
        }

        {
            let mut config = gix_config::File::new(gix_config::file::Metadata {
                path: Some(config2_path.clone()),
                source: Source::Api,
                level: 0,
                trust: Trust::Full,
            });

            config.set_raw_value(&"remote.qux", "hornets").unwrap();
            config.set_raw_value(&"remote.bar", "fire ants").unwrap();

            config
                .write_to(&mut std::fs::File::create(&config2_path).unwrap())
                .unwrap();
        }

        let config = gix_config::File::from_paths_metadata(
            [
                gix_config::file::Metadata {
                    path: Some(config1_path),
                    source: Source::Local,
                    level: 0,
                    trust: Trust::Full,
                },
                gix_config::file::Metadata {
                    path: Some(config2_path),
                    source: Source::Api,
                    level: 0,
                    trust: Trust::Full,
                },
            ]
            .iter()
            .cloned(),
            gix_config::file::init::Options::default(),
        )
        .unwrap()
        .unwrap();

        assert_eq!("bees", *config.string("remote.foo").unwrap());
        assert_eq!("fire ants", *config.string("remote.bar").unwrap());
        assert_eq!("hornets", *config.string("remote.qux").unwrap());
    }

    #[test]
    fn test_parse_rejects_unknown_version() {
        let tmp = tempfile::Builder::new()
            .prefix("rust-test")
            .tempdir()
            .unwrap();
        let out = tmp.path().join("git_config");
        let err = parse_into_file("1abc", "origin", &out).expect_err("must fail");
        assert!(format!("{err}").contains("unknown version or invalid"));
    }

    #[test]
    fn test_parse_rejects_invalid_base64() {
        let tmp = tempfile::Builder::new()
            .prefix("rust-test")
            .tempdir()
            .unwrap();
        let out = tmp.path().join("git_config");
        let err = parse_into_file("0!!!", "origin", &out).expect_err("must fail");
        assert!(format!("{err}").contains("base64"));
    }

    #[test]
    fn test_parse_rejects_invalid_brotli_or_bincode() {
        let tmp = tempfile::Builder::new()
            .prefix("rust-test")
            .tempdir()
            .unwrap();
        let out = tmp.path().join("git_config");
        let not_brotli = format!("0{}", URL_SAFE_NO_PAD.encode(b"not-brotli-or-bincode"));
        let err = parse_into_file(&not_brotli, "origin", &out).expect_err("must fail");
        assert!(format!("{err}").contains("deserialize"));
    }
}
