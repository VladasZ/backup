use std::env;
use std::io::ErrorKind;
use std::process::{Child, ExitStatus};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use humantime::format_duration;
use tracing::warn;

const DEFAULT_STALL_TIMEOUT: Duration = Duration::from_secs(15 * 60);
const CHECK_INTERVAL: Duration = Duration::from_secs(1);
const WAIT_POLL: Duration = Duration::from_millis(50);

// A remote that keeps the SSH connection alive while its disk hangs would block the
// serial queue forever, and SSH keepalives only catch a dead network. The watchdog
// kills the SSH child once no byte has moved for the timeout, so the blocked read or
// write fails and every existing error path handles the dead connection as usual.
pub struct Stall {
    started: Instant,
    timeout: Duration,
    last_progress_ms: AtomicU64,
    fired: AtomicBool,
    disarmed: AtomicBool,
}

impl Stall {
    pub fn arm(child: &Arc<Mutex<Child>>) -> Arc<Self> {
        let stall = Arc::new(Self::new(stall_timeout()));
        watch(Arc::clone(&stall), Arc::clone(child));
        stall
    }

    fn new(timeout: Duration) -> Self {
        Self {
            started: Instant::now(),
            timeout,
            last_progress_ms: AtomicU64::new(0),
            fired: AtomicBool::new(false),
            disarmed: AtomicBool::new(false),
        }
    }

    pub fn bump(&self) {
        self.last_progress_ms
            .store(as_millis(self.started.elapsed()), Ordering::SeqCst);
    }

    pub fn disarm(&self) {
        self.disarmed.store(true, Ordering::SeqCst);
    }

    pub fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    pub fn message(&self) -> String {
        format!(
            "remote made no progress for {}; the connection was terminated",
            format_duration(self.timeout)
        )
    }

    fn stalled_at(&self, elapsed: Duration) -> bool {
        let last = Duration::from_millis(self.last_progress_ms.load(Ordering::SeqCst));
        elapsed.saturating_sub(last) >= self.timeout
    }
}

fn as_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn watch(stall: Arc<Stall>, child: Arc<Mutex<Child>>) {
    let interval = stall.timeout.min(CHECK_INTERVAL);
    thread::spawn(move || {
        loop {
            thread::sleep(interval);
            if stall.disarmed.load(Ordering::SeqCst) {
                return;
            }
            if !stall.stalled_at(stall.started.elapsed()) {
                continue;
            }
            stall.fired.store(true, Ordering::SeqCst);
            if let Err(error) = kill_child(&child) {
                warn!(%error, "could not terminate a stalled SSH command");
            }
            return;
        }
    });
}

pub fn kill_child(child: &Mutex<Child>) -> Result<()> {
    let mut child = child
        .lock()
        .map_err(|_| anyhow!("SSH child lock is poisoned"))?;
    match child.kill() {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == ErrorKind::InvalidInput => Ok(()),
        Err(error) => Err(error).context("terminate SSH command"),
    }
}

// The child is polled instead of blocking in wait, so the watchdog can take the lock
// and kill it while the controller is still waiting for the exit.
pub fn wait_child(child: &Mutex<Child>) -> Result<ExitStatus> {
    loop {
        let status = child
            .lock()
            .map_err(|_| anyhow!("SSH child lock is poisoned"))?
            .try_wait()
            .context("wait for SSH command")?;
        if let Some(status) = status {
            return Ok(status);
        }
        thread::sleep(WAIT_POLL);
    }
}

// Tests shorten the timeout to seconds through this hidden override.
fn stall_timeout() -> Duration {
    env::var("BACKUP_STALL_TIMEOUT_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_STALL_TIMEOUT)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::Stall;

    #[test]
    fn fires_only_after_the_timeout_passes_without_progress() {
        let stall = Stall::new(Duration::from_secs(10));
        assert!(!stall.stalled_at(Duration::from_secs(9)));
        assert!(stall.stalled_at(Duration::from_secs(10)));

        stall.last_progress_ms.store(8_000, Ordering::SeqCst);
        assert!(!stall.stalled_at(Duration::from_secs(17)));
        assert!(stall.stalled_at(Duration::from_secs(18)));
    }
}
