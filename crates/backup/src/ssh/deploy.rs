use std::collections::HashMap;
use std::env::current_exe;
use std::fs::File;
use std::io::{BufReader, Read, Write, copy};
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;

use anyhow::{Context, Result, anyhow, bail};
use blake3::Hasher;
use tracing::info;

use super::session::ssh_base;
use super::stall::{Stall, wait_child};
use crate::location::SshLocation;

const REMOTE_DIRECTORY: &str = ".cache/backup";
const INSTALLED: &str = "backup";

// The controller copies its own binary to the remote instead of trusting one
// installed there, so the two ends cannot run different protocol versions. The
// file name carries a hash of the binary, so a new build lands next to the old
// one and a session already running the old one is undisturbed.
pub fn agent_argv(remote: &SshLocation) -> Result<Vec<String>> {
    let key = host_key(remote);
    if let Some(argv) = locked(resolved()).get(&key) {
        return Ok(argv.clone());
    }
    let argv = resolve(remote)?;
    locked(resolved()).insert(key, argv.clone());
    Ok(argv)
}

fn resolved() -> &'static Mutex<HashMap<String, Vec<String>>> {
    static RESOLVED: OnceLock<Mutex<HashMap<String, Vec<String>>>> = OnceLock::new();
    RESOLVED.get_or_init(|| Mutex::new(HashMap::new()))
}

// A panic in another thread while the map was held leaves it poisoned but not
// damaged, since every write is a single insert.
fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn host_key(remote: &SshLocation) -> String {
    format!("{}:{}", remote.target(), remote.port.unwrap_or(22))
}

fn resolve(remote: &SshLocation) -> Result<Vec<String>> {
    let binary = current_exe().context("locate the running backup binary")?;
    let path = format!("$HOME/{REMOTE_DIRECTORY}/agent-{}", fingerprint(&binary)?);
    if runs_on_remote(remote, &path)? {
        return Ok(vec![path, "agent".to_owned()]);
    }
    upload(remote, &binary, &path)?;
    // The copy is proved by running it rather than by comparing platforms. A
    // remote can report the same kernel and machine and still not run this
    // binary, for example a NixOS build sent to a plain Linux host, where the
    // hardcoded loader path under /nix/store does not exist.
    if runs_on_remote(remote, &path)? {
        return Ok(vec![path, "agent".to_owned()]);
    }
    info!(
        host = remote.host,
        "the copied backup agent does not run there, so the installed one is used"
    );
    Ok(vec![INSTALLED.to_owned(), "agent".to_owned()])
}

fn fingerprint(binary: &Path) -> Result<String> {
    let mut file = BufReader::new(
        File::open(binary)
            .with_context(|| format!("read the backup binary {}", binary.display()))?,
    );
    let mut hasher = Hasher::new();
    copy(&mut file, &mut hasher).context("hash the backup binary")?;
    let hex = hasher.finalize().to_hex();
    Ok(hex[..16].to_owned())
}

// Both steps run under the stall watchdog like every other connection. A
// remote that accepts SSH and then hangs would otherwise hold the serial queue
// until the connection dies, which can be never.
fn runs_on_remote(remote: &SshLocation, path: &str) -> Result<bool> {
    let child = ssh_base(remote)
        .args([path, "--version"])
        // The probe must not read the caller's input, which for the daemon is
        // the stream a session is about to use.
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("try the backup agent on {}", remote.host))?;
    let child = Arc::new(Mutex::new(child));
    let stall = Stall::arm(&child);
    let status = wait_child(&child)?;
    stall.disarm();
    if stall.fired() {
        bail!(
            "try the backup agent on {}: {}",
            remote.host,
            stall.message()
        );
    }
    Ok(status.success())
}

fn upload(remote: &SshLocation, binary: &Path, path: &str) -> Result<()> {
    info!(
        host = remote.host,
        "copying the backup agent to the remote host"
    );
    let partial = format!("{path}.$$.partial");
    let script = format!(
        "set -e; mkdir -p $HOME/{REMOTE_DIRECTORY}; cat > {partial}; chmod 700 {partial}; mv {partial} {path}"
    );
    let mut child: Child = ssh_base(remote)
        .args(["sh", "-c", &single_quote(&script)])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("copy the backup agent to {}", remote.host))?;
    let mut stdin = child
        .stdin
        .take()
        .context("the copy command has no standard input")?;
    let mut stderr = child
        .stderr
        .take()
        .context("the copy command has no standard error")?;
    let errors = thread::spawn(move || {
        let mut message = String::new();
        stderr.read_to_string(&mut message).map(|_| message)
    });
    let child = Arc::new(Mutex::new(child));
    let stall = Stall::arm(&child);
    let sent = (|| -> Result<()> {
        let mut file = BufReader::new(
            File::open(binary)
                .with_context(|| format!("read the backup binary {}", binary.display()))?,
        );
        let mut buffer = vec![0; 256 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            stdin.write_all(&buffer[..read])?;
            stall.bump();
        }
        stdin.flush()?;
        Ok(())
    })();
    drop(stdin);
    let status = wait_child(&child)?;
    stall.disarm();
    let message = errors
        .join()
        .map_err(|_| anyhow!("the stderr reader panicked"))?
        .context("read the copy command's errors")?;
    if stall.fired() {
        bail!(
            "copy the backup agent to {}: {}",
            remote.host,
            stall.message()
        );
    }
    sent.with_context(|| format!("send the backup agent to {}", remote.host))?;
    if !status.success() {
        bail!(
            "copy the backup agent to {}: {}",
            remote.host,
            message.trim()
        );
    }
    Ok(())
}

// ssh joins its command arguments with spaces and the remote login shell parses
// the result, so the script has to survive one round of shell parsing intact.
fn single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{fingerprint, single_quote};

    #[test]
    fn a_quoted_script_survives_embedded_quotes() {
        assert_eq!(single_quote("echo 'hi'"), r"'echo '\''hi'\'''");
    }

    #[test]
    fn a_fingerprint_is_sixteen_hex_characters() {
        let binary = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let hash = fingerprint(&binary).unwrap();
        assert_eq!(hash.len(), 16);
        assert!(hash.chars().all(|character| character.is_ascii_hexdigit()));
    }
}
