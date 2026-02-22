extern crate recursive_remote;

use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use assert_cmd::prelude::*;
use eseb::KeyMaterial;
use gix::diff::object::bstr::BStr;
use predicates::prelude::*;
use recursive_remote::config::{ConfigKey, EncryptionKeys, EncryptionKeysInner};

fn git(bin_dir: &Path) -> std::process::Command {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let mut combined_path = OsString::from(bin_dir.as_os_str());
    combined_path.push(":");
    combined_path.push(path);

    let mut c = std::process::Command::new("git");
    c.env("PATH", combined_path)
        .env("GIT_COMMITTER_EMAIL", "you@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "you@example.com")
        .env("GIT_AUTHOR_NAME", "Test User")
        .arg("-c")
        .arg("init.defaultBranch=main");
    c
}

fn set_config(flavor: &Flavor, config: &mut gix_config::File<'static>, keys: &EncryptionKeys) {
    let subsection = flavor.remote_name();
    let subsection: &BStr = subsection.as_bytes().into();

    config
        .set_raw_value_by("remote", Some(subsection), ConfigKey::MaxObjectSize, "30")
        .unwrap();
    config
        .set_raw_value_by(
            "remote",
            Some(subsection),
            ConfigKey::RemoteBranch,
            flavor.branch_name(),
        )
        .unwrap();

    if let Some(key) = keys.namespace_key() {
        config
            .set_raw_value_by(
                "remote",
                Some(subsection),
                ConfigKey::NamespaceNaclKey,
                key.serialize_to_string().as_str(),
            )
            .unwrap();
    }

    if let Some(key) = keys.state_key() {
        config
            .set_raw_value_by(
                "remote",
                Some(subsection),
                ConfigKey::StateNaclKey,
                key.serialize_to_string().as_str(),
            )
            .unwrap();
    }
}

fn set_common_config(flavor: &Flavor, config: &mut gix_config::File, url: &str) {
    let subsection = flavor.remote_name();
    let subsection: &BStr = subsection.as_bytes().into();

    config
        .set_raw_value_by("remote", Some(subsection), "url", url)
        .unwrap();

    config
        .set_raw_value_by(
            "remote",
            Some(subsection),
            "fetch",
            "+refs/heads/*:refs/remotes/origin/*",
        )
        .unwrap();
}

enum Flavor {
    Plain,
    Encrypted,
}

impl Flavor {
    fn branch_name(&self) -> &'static str {
        match self {
            Flavor::Plain => "clear-branch",
            Flavor::Encrypted => "crypt-branch",
        }
    }

    fn remote_name(&self) -> &'static str {
        match self {
            Flavor::Plain => "clear",
            Flavor::Encrypted => "crypt",
        }
    }

    fn gen_keys(&self) -> EncryptionKeys {
        match self {
            Flavor::Encrypted => {
                let namespace_key = eseb::SymmetricKey::gen_key().unwrap();
                let state_key = eseb::SymmetricKey::gen_key().unwrap();
                let inner = Some(EncryptionKeysInner {
                    namespace_key,
                    state_key,
                });
                EncryptionKeys { inner }
            }
            Flavor::Plain => EncryptionKeys { inner: None },
        }
    }
}

#[test]
fn roundtrip_cleartext_embed_initial_sync() {
    scenario_initial_sync(&Flavor::Plain, true);
}

#[test]
fn roundtrip_crypttext_embed_initial_sync() {
    scenario_initial_sync(&Flavor::Encrypted, true);
}

#[test]
fn roundtrip_cleartext_initial_sync() {
    scenario_initial_sync(&Flavor::Plain, false);
}

#[test]
fn roundtrip_crypttext_initial_sync() {
    scenario_initial_sync(&Flavor::Encrypted, false);
}

#[test]
fn roundtrip_cleartext_embed_conflict_rejected() {
    scenario_conflict_rejected(&Flavor::Plain, true);
}

#[test]
fn roundtrip_crypttext_embed_conflict_rejected() {
    scenario_conflict_rejected(&Flavor::Encrypted, true);
}

#[test]
fn roundtrip_cleartext_conflict_rejected() {
    scenario_conflict_rejected(&Flavor::Plain, false);
}

#[test]
fn roundtrip_crypttext_conflict_rejected() {
    scenario_conflict_rejected(&Flavor::Encrypted, false);
}

#[test]
fn roundtrip_cleartext_merge_roundtrip() {
    scenario_merge_roundtrip(&Flavor::Plain, false);
}

#[test]
fn roundtrip_crypttext_merge_roundtrip() {
    scenario_merge_roundtrip(&Flavor::Encrypted, false);
}

#[test]
fn roundtrip_cleartext_churn() {
    scenario_churn(&Flavor::Plain, false);
}

#[test]
fn roundtrip_crypttext_churn() {
    scenario_churn(&Flavor::Encrypted, false);
}

struct ScenarioHarness {
    _tempdir: assert_fs::TempDir,
    bin_dir: PathBuf,
    workdir1: PathBuf,
    workdir2: PathBuf,
    remote_name: &'static str,
}

impl ScenarioHarness {
    fn new(flavor: &Flavor, embed_config: bool) -> ScenarioHarness {
        let tempdir = assert_fs::TempDir::new().unwrap();
        let tmp_path = tempdir.path();
        let bin_dir = tmp_path.join("bin");

        // This is an integration test that actually runs git, which is then
        // dependent on finding a compiled binary. This tries to set that up, but is
        // brittle.
        get_binary(&bin_dir);

        let mut user_repo1 = gix::init(tmp_path.join("user_repo1")).unwrap();
        let mut user_repo2 = gix::init(tmp_path.join("user_repo2")).unwrap();
        let workdir1 = user_repo1.workdir().unwrap().to_owned();
        let workdir2 = user_repo2.workdir().unwrap().to_owned();
        let upstream_repo = gix::init_bare(tmp_path.join("upstream_repo")).unwrap();

        let keys = flavor.gen_keys();

        let upstream = upstream_repo.path().to_string_lossy();
        let base_url = if embed_config {
            format!("file://{}", upstream)
        } else {
            format!("recursive::file://{}", upstream)
        };

        let url = if embed_config {
            let path = tmp_path.join("embed");
            let mut config = gix_config::File::new(gix_config::file::Metadata::default());
            set_config(flavor, &mut config, &keys);
            config
                .write_to(&mut std::fs::File::create(&path).unwrap())
                .unwrap();
            let embedded = recursive_remote::embedded_config::embed_file(&path).unwrap();
            assert_eq!(embedded.len(), 1);
            let embedded = embedded.into_values().next().unwrap().0;
            format!("recursive::{}:{}", &embedded, &base_url)
        } else {
            let mut config1 = user_repo1.config_snapshot_mut();
            let mut config2 = user_repo2.config_snapshot_mut();
            set_config(flavor, &mut config1, &keys);
            set_config(flavor, &mut config2, &keys);
            base_url
        };

        let config_path_1 = user_repo1.path().join("config");
        let config_path_2 = user_repo2.path().join("config");

        {
            let mut config1 = user_repo1.config_snapshot_mut();
            let mut config2 = user_repo2.config_snapshot_mut();
            set_common_config(flavor, &mut config1, &url);
            set_common_config(flavor, &mut config2, &url);

            config1
                .write_to(&mut std::fs::File::create(&config_path_1).unwrap())
                .unwrap();
            config2
                .write_to(&mut std::fs::File::create(&config_path_2).unwrap())
                .unwrap();
        }

        pretty_print(
            git(&bin_dir)
                .current_dir(&workdir1)
                .arg("branch")
                .arg("-m")
                .arg("main"),
        );

        pretty_print(
            git(&bin_dir)
                .current_dir(&workdir2)
                .arg("branch")
                .arg("-m")
                .arg("main"),
        );

        ScenarioHarness {
            _tempdir: tempdir,
            bin_dir,
            workdir1,
            workdir2,
            remote_name: flavor.remote_name(),
        }
    }

    fn content(n: usize) -> String {
        format!("hello hello hello hello hello hello {}", n)
    }

    fn commit_file_contents(&self, n: usize, workdir: &Path, contents: &str) {
        let name = format!("file{}", n);
        let path = workdir.join(&name);

        std::fs::File::create(&path)
            .unwrap()
            .write_all(contents.as_bytes())
            .unwrap();

        git(&self.bin_dir)
            .current_dir(workdir)
            .arg("add")
            .arg(&name)
            .assert()
            .success();

        git(&self.bin_dir)
            .current_dir(workdir)
            .arg("commit")
            .arg("-m")
            .arg(format!("commit file{}", n))
            .assert()
            .success();
    }

    fn commit_file(&self, n: usize, workdir: &Path) {
        self.commit_file_contents(n, workdir, &Self::content(n));
    }

    fn push_through(&self, source: &Path, dest: &Path) {
        push_through(self.remote_name, &self.bin_dir, source, dest);
    }

    fn assert_file(&self, workdir: &Path, n: usize) {
        read_file(workdir, &format!("file{}", n), &Self::content(n));
    }

    fn establish_two_way_sync(&self) {
        self.commit_file(1, &self.workdir1);
        self.commit_file(2, &self.workdir1);
        self.commit_file(3, &self.workdir1);

        self.push_through(&self.workdir1, &self.workdir2);
        self.assert_file(&self.workdir2, 2);

        self.commit_file(4, &self.workdir2);
        self.push_through(&self.workdir2, &self.workdir1);
        self.assert_file(&self.workdir1, 4);
    }

    fn run_merge_roundtrip(&self) {
        self.commit_file(5, &self.workdir2);
        self.commit_file(6, &self.workdir2);
        self.commit_file(7, &self.workdir2);
        self.commit_file(8, &self.workdir1);

        self.push_through(&self.workdir1, &self.workdir2);
        self.push_through(&self.workdir2, &self.workdir1);

        self.assert_file(&self.workdir1, 7);
        self.assert_file(&self.workdir2, 7);
        self.assert_file(&self.workdir1, 8);
        self.assert_file(&self.workdir2, 8);
    }

    fn run_conflict_rejected(&self) {
        self.commit_file_contents(9, &self.workdir1, "from");
        self.commit_file_contents(9, &self.workdir2, "to");

        pretty_print(
            git(&self.bin_dir)
                .current_dir(&self.workdir1)
                .arg("push")
                .arg(self.remote_name)
                .arg("main:main"),
        );

        git(&self.bin_dir)
            .current_dir(&self.workdir2)
            .arg("push")
            .arg(self.remote_name)
            .arg("main:main")
            .assert()
            .failure()
            .stderr(
                predicate::str::contains("Updates were rejected because the remote contains work")
                    .or(predicate::str::contains("failed to push some refs"))
                    .or(predicate::str::contains("remote rejected"))
                    .or(predicate::str::contains("rejected")),
            );
    }

    fn run_churn(&self) {
        for j in 100..115 {
            self.commit_file(j, &self.workdir1);
            self.push_through(&self.workdir1, &self.workdir2);
            self.assert_file(&self.workdir2, j);
        }
    }
}

fn scenario_initial_sync(flavor: &Flavor, embed_config: bool) {
    let harness = ScenarioHarness::new(flavor, embed_config);
    harness.establish_two_way_sync();
}

fn scenario_conflict_rejected(flavor: &Flavor, embed_config: bool) {
    let harness = ScenarioHarness::new(flavor, embed_config);
    harness.establish_two_way_sync();
    harness.run_conflict_rejected();
}

fn scenario_merge_roundtrip(flavor: &Flavor, embed_config: bool) {
    let harness = ScenarioHarness::new(flavor, embed_config);
    harness.establish_two_way_sync();
    harness.run_merge_roundtrip();
}

fn scenario_churn(flavor: &Flavor, embed_config: bool) {
    let harness = ScenarioHarness::new(flavor, embed_config);
    harness.establish_two_way_sync();
    harness.run_churn();
}

fn read_file(workdir: &Path, name: &str, contents: &str) {
    let mut fd = std::fs::File::open(workdir.join(name)).unwrap();
    let mut s = String::default();
    fd.read_to_string(&mut s).unwrap();
    assert_eq!(&s, contents);
}

fn push_through(remote_name: &str, bin_dir: &Path, source: &Path, dest: &Path) {
    eprintln!(
        "Push through in dir {} with rn {}",
        source.display(),
        remote_name
    );
    pretty_print(
        git(bin_dir)
            .current_dir(source)
            .arg("push")
            .arg(remote_name)
            .arg("main:main"),
    );

    pretty_print(
        git(bin_dir)
            .current_dir(dest)
            .arg("pull")
            .arg("--rebase=false")
            .arg(remote_name),
    );
}

fn pretty_print(command: &mut std::process::Command) {
    command.assert().success();
}

fn get_binary(bin_dir: &Path) {
    let b = assert_cmd::cargo::cargo_bin!("git-remote-recursive");
    std::fs::create_dir(bin_dir).unwrap();
    std::fs::copy(b, bin_dir.join("git-remote-recursive"))
        .expect("copy git-remote-recursive for git helper discovery");
}
