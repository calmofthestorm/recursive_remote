extern crate recursive_remote;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use eseb::KeyMaterial;
use recursive_remote::config::{
    write_config, write_config_i64, ConfigKey, EncryptionKeys, EncryptionKeysInner,
};

// FIXME: Share with non-test code.
fn debug_stream_message<S: Read>(stream: Option<S>, sn: &'static str) -> Result<String> {
    match stream {
        Some(mut s) => {
            let mut st = Vec::default();
            s.read_to_end(&mut st).context("read").context(sn)?;
            match std::str::from_utf8(&st) {
                Ok(m) => Ok(m.to_string()),
                Err(_) => Ok(format!("<utf error> {:?}", &st)),
            }
        }
        None => Ok(format!("<no {}>", sn)),
    }
}

// FIXME: Share with non-test code.
pub fn execute_subprocess2(
    command: &mut std::process::Command,
) -> anyhow::Result<std::process::Output> {
    let output = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()?;

    if output.status.success() {
        Ok(output)
    } else {
        Err(anyhow::Error::msg(format!(
            "subprocess failed.\n---STDOUT---\n{}\n\n---STDERR---\n\n{}\n",
            debug_stream_message(Some(output.stdout.as_slice()), "stdout")?,
            debug_stream_message(Some(output.stderr.as_slice()), "stderr")?,
        )))
    }
}

fn git(bin_dir: &Path) -> std::process::Command {
    let mut c = std::process::Command::new("git");
    c.env_clear()
        .env("PATH", bin_dir)
        .env("GIT_COMMITTER_EMAIL", "you@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "you@example.com")
        .env("GIT_AUTHOR_NAME", "Test User");
    c
}

fn set_config(
    flavor: &Flavor,
    config: &mut git2::Config,
    bin_dir: &Path,
    workdir: &Path,
    upstream: &Path,
    keys: &EncryptionKeys,
) {
    let upstream = upstream.to_string_lossy();
    let url = format!("recursive::file://{}", upstream);
    execute_subprocess2(
        git(bin_dir)
            .current_dir(&workdir)
            .arg("remote")
            .arg("add")
            .arg(flavor.remote_name())
            .arg(&url),
    )
    .expect("git remote add");

    write_config_i64(flavor.remote_name(), ConfigKey::MaxObjectSize, config, 30).unwrap();

    write_config(
        flavor.remote_name(),
        ConfigKey::RemoteBranch,
        config,
        flavor.branch_name(),
    )
    .unwrap();

    if let Some(key) = keys.namespace_key() {
        write_config(
            flavor.remote_name(),
            ConfigKey::NamespaceNaclKey,
            config,
            &key.serialize_to_string(),
        )
        .unwrap();
    }

    if let Some(key) = keys.state_key() {
        write_config(
            flavor.remote_name(),
            ConfigKey::StateNaclKey,
            config,
            &key.serialize_to_string(),
        )
        .unwrap();
    }
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
fn roundtrip_cleartext() {
    roundtrip(&Flavor::Plain, /*do_conflict=*/ false);
}

#[test]
fn roundtrip_cleartext_conflict() {
    roundtrip(&Flavor::Plain, /*do_conflict=*/ true);
}

#[test]
fn roundtrip_crypttext() {
    roundtrip(&Flavor::Encrypted, /*do_conflict=*/ false);
}

#[test]
fn roundtrip_crypttext_conflict() {
    roundtrip(&Flavor::Encrypted, /*do_conflict=*/ true);
}

fn roundtrip(flavor: &Flavor, do_conflict: bool) {
    let td = tempdir::TempDir::new("rust-test").unwrap();
    let bin_dir = td.path().join("bin");

    // This is an integration test that actually runs git, which is then
    // dependent on finding a compiled binary. This tries to set that up, but is
    // brittle.
    get_binary(&bin_dir);

    let user_repo1 = git2::Repository::init(&td.path().join("user_repo1")).unwrap();
    let user_repo2 = git2::Repository::init(&td.path().join("user_repo2")).unwrap();
    let workdir1 = &user_repo1.workdir().unwrap();
    let workdir2 = &user_repo2.workdir().unwrap();
    let upstream_repo = git2::Repository::init_bare(&td.path().join("upstream_repo")).unwrap();

    let keys = flavor.gen_keys();

    set_config(
        flavor,
        &mut user_repo1.config().unwrap(),
        &bin_dir,
        &workdir1,
        &upstream_repo.path(),
        &keys,
    );

    set_config(
        flavor,
        &mut user_repo2.config().unwrap(),
        &bin_dir,
        &workdir2,
        &upstream_repo.path(),
        &keys,
    );

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

    let commit_file_contents = |n, workdir: &Path, contents: &str| {
        let name = format!("file{}", n);
        let f1_path = workdir.join(&name);

        std::fs::File::create(&f1_path)
            .unwrap()
            .write_all(contents.as_bytes())
            .unwrap();

        execute_subprocess2(git(&bin_dir).current_dir(&workdir).arg("add").arg(&name))
            .expect("git add");

        execute_subprocess2(
            git(&bin_dir)
                .current_dir(&workdir)
                .arg("commit")
                .arg("-m")
                .arg(&format!("commit file{}", n)),
        )
        .expect("git commit");
    };

    let content = |n| format!("hello hello hello hello hello hello {}", n);
    let commit_file = |n, workdir: &Path| commit_file_contents(n, workdir, &content(n));

    commit_file(1, &workdir1);
    commit_file(2, &workdir1);
    commit_file(3, &workdir1);

    // Push commits from repo1 to upstream, thence to repo2.
    push_through(flavor.remote_name(), &bin_dir, &workdir1, &workdir2);

    // Verify one of the files.
    read_file(&workdir2, "file2", &content(2));

    // Commit a new file on the second repo.
    commit_file(4, &workdir2);

    push_through(flavor.remote_name(), &bin_dir, &workdir2, &workdir1);

    // Read the file created in repo2 in repo1.
    read_file(&workdir1, "file4", &content(4));

    // Commit some files on both.
    commit_file(5, &workdir2);
    commit_file(6, &workdir2);
    commit_file(7, &workdir2);
    commit_file(8, &workdir1);

    // Do a round trip, which should trigger a merge.
    push_through(flavor.remote_name(), &bin_dir, &workdir1, &workdir2);
    push_through(flavor.remote_name(), &bin_dir, &workdir2, &workdir1);

    // Verify the merge worked.
    read_file(&workdir1, "file7", &content(7));
    read_file(&workdir2, "file7", &content(7));
    read_file(&workdir1, "file8", &content(8));
    read_file(&workdir2, "file8", &content(8));

    if do_conflict {
        // Create a conflict on the upstream.
        commit_file_contents(9, &workdir1, "from");
        commit_file_contents(9, &workdir2, "to");

        pretty_print(
            git(&bin_dir)
                .current_dir(&workdir1)
                .arg("push")
                .arg(flavor.remote_name())
                .arg("main:main"),
        );

        let conflict = git(&bin_dir)
            .current_dir(&workdir2)
            .arg("push")
            .arg(flavor.remote_name())
            .arg("main:main")
            .output()
            .unwrap();

        assert!(!conflict.status.success());

        let stderr = std::str::from_utf8(&conflict.stderr).unwrap();
        assert!(stderr.contains("Updates were rejected because the remote contains work"));
    } else {
        // Do changes in a loop to catch certain classes of race conditions.
        for j in 100..115 {
            commit_file(j, &workdir1);
            push_through(flavor.remote_name(), &bin_dir, &workdir1, &workdir2);
            read_file(&workdir2, &format!("file{}", j), &content(j));
        }
    }
}

fn read_file(workdir: &Path, name: &str, contents: &str) {
    let mut fd = std::fs::File::open(workdir.join(&name)).unwrap();
    let mut s = String::default();
    fd.read_to_string(&mut s).unwrap();
    assert_eq!(&s, contents);
}

fn push_through(remote_name: &str, bin_dir: &Path, source: &Path, dest: &Path) {
    pretty_print(
        git(&bin_dir)
            .current_dir(source)
            .arg("push")
            .arg(remote_name)
            .arg("main:main"),
    );

    pretty_print(
        git(&bin_dir)
            .current_dir(dest)
            .arg("pull")
            .arg("--rebase=false")
            .arg(remote_name),
    );
}

fn pretty_print(command: &mut std::process::Command) {
    let output = command.output().unwrap();
    if output.status.success() {
        return;
    }

    eprintln!("A git command failed. The output is above. Please keep in mind that this is output from recursive_remote that git ran in a subprocess, and also this test ran that git in a subprocess. We have captured the git output and printed it above.");

    std::io::stderr().lock().write_all(&output.stderr).unwrap();

    panic!("A git command failed.");
}

#[cfg(debug_assertions)]
fn build_name() -> &'static str {
    "debug"
}

#[cfg(not(debug_assertions))]
fn build_name() -> &'static str {
    "release"
}

fn get_binary(bin_dir: &Path) {
    let b = PathBuf::from(format!("../target/{}/git-remote-recursive", build_name()));
    std::fs::create_dir(bin_dir).unwrap();
    std::fs::copy(&b, bin_dir.join("git-remote-recursive")).expect("For this integration test to pass, you'll need to build the binary and have it be available in relpath ../target/debug|release/git-remote-recursive.");
    std::fs::copy("/usr/bin/git", bin_dir.join("git")).expect("unable to copy git");
}
