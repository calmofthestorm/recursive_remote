use std::path::Path;

use assert_cmd::prelude::*;
use assert_fs::prelude::*;
use predicates::prelude::*;

fn git_command() -> std::process::Command {
    let mut c = std::process::Command::new("git");
    c.env("GIT_COMMITTER_EMAIL", "you@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "you@example.com")
        .env("GIT_AUTHOR_NAME", "Test User")
        .arg("-c")
        .arg("init.defaultBranch=main");
    c
}

fn git_init(path: &Path) {
    git_command().arg("init").arg(path).assert().success();
}

fn git_init_bare(path: &Path) {
    git_command()
        .arg("init")
        .arg("--bare")
        .arg(path)
        .assert()
        .success();
}

struct Paths {
    _tmp: assert_fs::TempDir,
    git_dir: std::path::PathBuf,
    remote_spec: String,
}

fn setup_paths() -> Paths {
    let tmp = assert_fs::TempDir::new().expect("tempdir");
    let user_repo = tmp.child("user_repo");
    let upstream = tmp.child("upstream_repo");
    git_init(user_repo.path());
    git_init_bare(upstream.path());
    let git_dir = user_repo.path().join(".git");
    let remote_spec = format!("file://{}", upstream.path().display());
    Paths {
        _tmp: tmp,
        git_dir,
        remote_spec,
    }
}

#[test]
fn protocol_capabilities_reports_expected_features() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("capabilities\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("push"))
        .stdout(predicate::str::contains("fetch"));
}

#[test]
fn protocol_invalid_push_spec_returns_error() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("push refs/heads/main\n\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Failed to push"))
        .stderr(
            predicate::str::contains("parse push specs")
                .or(predicate::str::contains("bad push spec dest")),
        );
}

#[test]
fn protocol_push_requires_blank_line_terminator() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("push refs/heads/main:refs/heads/main\nunexpected\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("push collect"))
        .stderr(predicate::str::contains(
            "expected blank line while collecting",
        ));
}

#[test]
fn protocol_fetch_requires_blank_line_terminator() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("fetch 0123456789012345678901234567890123456789 main\nunexpected\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("fetch collect"))
        .stderr(predicate::str::contains(
            "expected blank line while collecting",
        ));
}

#[test]
fn protocol_list_works_with_empty_state() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("list\n\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("\n"));
}

#[test]
fn protocol_fetch_without_oid_does_not_panic() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("fetch\n\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Failed to fetch"))
        .stderr(predicate::str::contains("panicked").not());
}

#[test]
fn protocol_unknown_command_is_ignored_and_loop_continues() {
    let paths = setup_paths();
    let mut cmd = assert_cmd::Command::new(assert_cmd::cargo::cargo_bin!("git-remote-recursive"));
    cmd.env("GIT_DIR", &paths.git_dir)
        .arg("origin")
        .arg(&paths.remote_spec)
        .write_stdin("unknown_command hello\ncapabilities\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("push"))
        .stdout(predicate::str::contains("fetch"));
}
