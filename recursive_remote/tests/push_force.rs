extern crate recursive_remote;

use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};

use assert_cmd::prelude::*;
use gix::diff::object::bstr::BStr;
use predicates::prelude::*;
use recursive_remote::config::ConfigKey;

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

struct Harness {
    _tmp: assert_fs::TempDir,
    bin_dir: PathBuf,
    workdir1: PathBuf,
    workdir2: PathBuf,
    remote_name: &'static str,
}

impl Harness {
    fn new() -> Self {
        let tmp = assert_fs::TempDir::new().expect("tempdir");
        let tmp_path = tmp.path();
        let bin_dir = tmp_path.join("bin");
        get_binary(&bin_dir);

        let mut user_repo1 = gix::init(tmp_path.join("user_repo1")).expect("init user_repo1");
        let mut user_repo2 = gix::init(tmp_path.join("user_repo2")).expect("init user_repo2");
        let upstream_repo = gix::init_bare(tmp_path.join("upstream_repo")).expect("init upstream");
        let workdir1 = user_repo1.workdir().expect("workdir1").to_owned();
        let workdir2 = user_repo2.workdir().expect("workdir2").to_owned();
        let upstream_url = format!("recursive::file://{}", upstream_repo.path().display());

        configure_remote(&mut user_repo1, "clear", &upstream_url);
        configure_remote(&mut user_repo2, "clear", &upstream_url);

        git(&bin_dir)
            .current_dir(&workdir1)
            .arg("branch")
            .arg("-m")
            .arg("main")
            .assert()
            .success();
        git(&bin_dir)
            .current_dir(&workdir2)
            .arg("branch")
            .arg("-m")
            .arg("main")
            .assert()
            .success();

        Self {
            _tmp: tmp,
            bin_dir,
            workdir1,
            workdir2,
            remote_name: "clear",
        }
    }

    fn commit_file(&self, workdir: &Path, name: &str, contents: &str, msg: &str) {
        let path = workdir.join(name);
        std::fs::File::create(path)
            .expect("create")
            .write_all(contents.as_bytes())
            .expect("write");
        git(&self.bin_dir)
            .current_dir(workdir)
            .arg("add")
            .arg(name)
            .assert()
            .success();
        git(&self.bin_dir)
            .current_dir(workdir)
            .arg("commit")
            .arg("-m")
            .arg(msg)
            .assert()
            .success();
    }

    fn push(&self, workdir: &Path, force: bool, spec: &str) -> assert_cmd::assert::Assert {
        let mut cmd = git(&self.bin_dir);
        cmd.current_dir(workdir).arg("push");
        if force {
            cmd.arg("--force");
        }
        cmd.arg(self.remote_name).arg(spec).assert()
    }
}

fn configure_remote(repo: &mut gix::Repository, remote_name: &str, url: &str) {
    let subsection: &BStr = remote_name.as_bytes().into();
    let config_path = repo.path().join("config");
    let mut config = repo.config_snapshot_mut();
    config
        .set_raw_value_by("remote", Some(subsection), "url", url)
        .expect("url");
    config
        .set_raw_value_by(
            "remote",
            Some(subsection),
            "fetch",
            "+refs/heads/*:refs/remotes/clear/*",
        )
        .expect("fetch");
    config
        .set_raw_value_by("remote", Some(subsection), ConfigKey::RemoteBranch, "main")
        .expect("remote branch");
    config
        .set_raw_value_by(
            "remote",
            Some(subsection),
            ConfigKey::Namespace,
            "push_force_ns",
        )
        .expect("namespace");
    config
        .set_raw_value_by("remote", Some(subsection), ConfigKey::MaxObjectSize, "30")
        .expect("max size");
    config
        .write_to(&mut std::fs::File::create(config_path).expect("open config"))
        .expect("write config");
}

fn get_binary(bin_dir: &Path) {
    let b = assert_cmd::cargo::cargo_bin!("git-remote-recursive");
    std::fs::create_dir(bin_dir).expect("create bin dir");
    std::fs::copy(&b, bin_dir.join("git-remote-recursive"))
        .expect("copy git-remote-recursive for git helper discovery");
}

#[test]
fn non_fast_forward_push_is_rejected_without_force_and_allowed_with_force() {
    let h = Harness::new();

    h.commit_file(&h.workdir1, "base.txt", "base", "base");
    h.push(&h.workdir1, false, "main:main").success();

    git(&h.bin_dir)
        .current_dir(&h.workdir2)
        .arg("pull")
        .arg("--rebase=false")
        .arg(h.remote_name)
        .assert()
        .success();

    h.commit_file(&h.workdir1, "from1.txt", "from1", "from1");
    h.push(&h.workdir1, false, "main:main").success();

    h.commit_file(&h.workdir2, "from2.txt", "from2", "from2");
    h.push(&h.workdir2, false, "main:main").failure().stderr(
        predicate::str::contains("rejected")
            .or(predicate::str::contains("failed to push some refs")),
    );

    h.push(&h.workdir2, true, "main:main").success();
}

#[test]
fn tag_update_requires_force() {
    let h = Harness::new();

    h.commit_file(&h.workdir1, "base.txt", "base", "base");
    h.push(&h.workdir1, false, "main:main").success();

    git(&h.bin_dir)
        .current_dir(&h.workdir2)
        .arg("pull")
        .arg("--rebase=false")
        .arg(h.remote_name)
        .assert()
        .success();

    git(&h.bin_dir)
        .current_dir(&h.workdir1)
        .arg("tag")
        .arg("v1")
        .assert()
        .success();
    h.push(&h.workdir1, false, "refs/tags/v1:refs/tags/v1")
        .success();

    h.commit_file(&h.workdir2, "next.txt", "next", "next");
    git(&h.bin_dir)
        .current_dir(&h.workdir2)
        .arg("tag")
        .arg("-f")
        .arg("v1")
        .assert()
        .success();

    h.push(&h.workdir2, false, "refs/tags/v1:refs/tags/v1")
        .failure()
        .stderr(
            predicate::str::contains("rejected")
                .or(predicate::str::contains("failed to push some refs")),
        );
    h.push(&h.workdir2, true, "refs/tags/v1:refs/tags/v1")
        .success();
}
