use std::ffi::OsString;
use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};
use once_cell::sync::OnceCell;

struct Environment {
    ssh_agent_pid: Option<OsString>,
    ssh_auth_sock: Option<OsString>,
    git_ssh_command: Option<OsString>,
    git_ssh: Option<OsString>,
    git_askpass: Option<OsString>,
}

impl Environment {
    fn new() -> Environment {
        Environment {
            ssh_agent_pid: std::env::var_os("SSH_AGENT_PID"),
            ssh_auth_sock: std::env::var_os("SSH_AUTH_SOCK"),
            git_ssh_command: std::env::var_os("GIT_SSH_COMMAND"),
            git_ssh: std::env::var_os("GIT_SSH"),
            git_askpass: std::env::var_os("GIT_ASKPASS"),
        }
    }

    fn apply(&self, cmd: &mut std::process::Command) {
        Self::maybe_set(cmd, "SSH_AGENT_PID", self.ssh_agent_pid.as_ref());
        Self::maybe_set(cmd, "SSH_AUTH_SOCK", self.ssh_auth_sock.as_ref());
        Self::maybe_set(cmd, "GIT_SSH_COMMAND", self.git_ssh_command.as_ref());
        Self::maybe_set(cmd, "GIT_SSH", self.git_ssh.as_ref());
        Self::maybe_set(cmd, "GIT_ASKPASS", self.git_askpass.as_ref());
    }

    fn maybe_set(cmd: &mut std::process::Command, key: &str, value: Option<&OsString>) {
        if let Some(value) = value {
            cmd.env(key, value);
        }
    }
}

static CELL: OnceCell<Environment> = OnceCell::new();

pub fn git_command() -> std::process::Command {
    let environment = CELL.get_or_init(Environment::new);
    let mut cmd = std::process::Command::new("git");
    cmd.env_clear()
        .env("GIT_CONFIG_NOSYSTEM", "")
        .env("GIT_COMMITTER_EMAIL", "you@example.com")
        .env("GIT_COMMITTER_NAME", "Test User")
        .env("GIT_AUTHOR_EMAIL", "you@example.com")
        .env("GIT_AUTHOR_NAME", "Test User");
    environment.apply(&mut cmd);
    cmd
}

pub fn acquire_flock(lockfile: &Path) -> Result<file_lock::FileLock> {
    file_lock::FileLock::lock(
        lockfile,
        /*is_blocking=*/ true,
        file_lock::FileOptions::new()
            .write(true)
            .append(true)
            .create(true),
    )
    .with_context(|| format!("acquire_flock {}", lockfile.display()))
}

pub fn rr_signature() -> gix_actor::Signature {
    gix_actor::Signature {
        name: "Recursive Remote Default".into(),
        email: "recursive-remote@example.com".into(),
        time: gix_date::Time::now_local_or_utc(),
    }
}

pub fn anyhow_ref_commit(
    repo: &gix::Repository,
    ref_name: &str,
    msg: &str,
    tree: gix::ObjectId,
) -> anyhow::Result<gix_hash::ObjectId> {
    log::trace!("Commit to ref {:?}", ref_name);
    let sig = rr_signature();
    let mut committer_time = gix_date::parse::TimeBuf::default();
    let mut author_time = gix_date::parse::TimeBuf::default();
    let x = match repo
        .try_find_reference(ref_name)
        .with_context(|| format!("failed to lookup refname {} to oid", &ref_name))?
    {
        Some(mut r) => {
            let pid = r.peel_to_id()?.detach();
            log::trace!("Committing to {} with one parent {}", ref_name, pid);
            repo.commit_as(
                sig.to_ref(&mut committer_time),
                sig.to_ref(&mut author_time),
                ref_name,
                msg,
                tree,
                [pid].iter().copied(),
            )
        }
        None => {
            log::trace!("Committing to {} with zero parents", ref_name);
            let parents: &[gix::ObjectId; 0] = &[];
            repo.commit_as(
                sig.to_ref(&mut committer_time),
                sig.to_ref(&mut author_time),
                ref_name,
                msg,
                tree,
                parents.iter().copied(),
            )
        }
    }
    .with_context(|| format!("failed to commit tree {} to ref {}", &tree, &ref_name))
    .map(Into::into);
    x
}

pub fn peel_reference_to_commit<'a>(
    repo: &'a gix::Repository,
    ref_name: &str,
) -> anyhow::Result<Option<gix::Commit<'a>>> {
    match repo.try_find_reference(ref_name)? {
        Some(mut r) => {
            let commit_oid = r
                .peel_to_id()
                .with_context(|| format!("Failed to resolve reference {}", &ref_name))?;
            let commit = repo
                .find_commit(commit_oid.detach())
                .with_context(|| format!("Unable to find commit {:?}", &commit_oid))?;
            Ok(Some(commit))
        }
        None => Ok(None),
    }
}

pub fn open_create_bare_repository(path: &Path) -> anyhow::Result<gix::Repository> {
    match gix::open(&path) {
        Ok(r) => Ok(r),
        Err(_) => gix::init_bare(&path)
            .with_context(|| format!("failed to init bare repository in {}", path.display())),
    }
}

fn debug_stream_message<S: Read>(stream: Option<S>, sn: &'static str) -> anyhow::Result<String> {
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

pub fn wait_subprocess(command: &mut std::process::Child) -> Result<()> {
    let stdout = command.stdout.take();
    let stderr = command.stderr.take();
    let stdout = move || debug_stream_message(stdout, "stdout");
    let stderr = move || debug_stream_message(stderr, "stderr");

    let output = command.wait()?;

    if output.success() {
        Ok(())
    } else {
        Err(anyhow::Error::msg(format!(
            "subprocess failed.\n---STDOUT---\n{}\n\n---STDERR---\n\n{}\n",
            stdout()?,
            stderr()?
        )))
    }
}

pub fn git_gc_auto(path: &Path) -> Result<()> {
    if !std::process::Command::new("git")
        .arg("--git-dir")
        .arg(path)
        .arg("gc")
        .arg("--auto")
        .status()?
        .success()
    {
        anyhow::bail!("Failed to run git gc --auto.")
    } else {
        Ok(())
    }
}
