use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Error, Result, anyhow, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, from_str, from_value, to_writer};
use tracing::warn;

use super::stall::{Stall, kill_child, wait_child};
use crate::location::SshLocation;
use crate::protocol::{AgentRequest, PROTOCOL_VERSION, RESPONSE_PREFIX, ResponseEnvelope};

#[derive(Debug)]
pub struct RemoteError(pub String);

impl Display for RemoteError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "remote agent error: {}", self.0)
    }
}

impl std::error::Error for RemoteError {}

pub struct Session {
    child: Arc<Mutex<Child>>,
    stdin: Option<ChildStdin>,
    response: JoinHandle<Result<Value>>,
    stderr: JoinHandle<Result<String>>,
    stall: Option<Arc<Stall>>,
}

impl Session {
    pub fn start(remote: &SshLocation, request: &AgentRequest) -> Result<Self> {
        Self::start_inner(remote, request, false)
    }

    // Watched sessions move archive data, where the daemon's serial queue must not
    // hang on a remote that stops responding. Verify and prune stay unwatched, since
    // they are legitimately silent for as long as the remote needs.
    pub fn start_watched(remote: &SshLocation, request: &AgentRequest) -> Result<Self> {
        Self::start_inner(remote, request, true)
    }

    fn start_inner(remote: &SshLocation, request: &AgentRequest, watched: bool) -> Result<Self> {
        let mut child = ssh_command(remote)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("start SSH agent")?;
        let stderr = drain_stderr(&mut child)?;
        let stdout = child.stdout.take().context("SSH stdout is unavailable")?;
        let response = thread::spawn(move || read_response::<Value>(&mut BufReader::new(stdout)));
        let mut stdin = child.stdin.take().context("SSH stdin is unavailable")?;
        let child = Arc::new(Mutex::new(child));
        let stall = watched.then(|| Stall::arm(&child));
        let mut session = Self {
            child,
            stdin: None,
            response,
            stderr,
            stall,
        };
        if let Err(error) = write_request(&mut stdin, request) {
            drop(stdin);
            return Err(session.abort().map_or(error, |detail| anyhow!(detail)));
        }
        session.stdin = Some(stdin);
        Ok(session)
    }

    pub fn stdin(&mut self) -> Result<&mut ChildStdin> {
        self.stdin.as_mut().context("SSH stdin is already closed")
    }

    pub fn bump(&self) {
        if let Some(stall) = &self.stall {
            stall.bump();
        }
    }

    fn stall_note(&self) -> Option<String> {
        self.stall
            .as_ref()
            .filter(|stall| stall.fired())
            .map(|stall| stall.message())
    }

    pub fn finish(mut self) -> Result<Value> {
        drop(self.stdin.take());
        let status = wait_child(&self.child)?;
        if let Some(stall) = &self.stall {
            stall.disarm();
        }
        let note = self.stall_note();
        let response = join(self.response)?;
        let stderr = join(self.stderr)??;
        match response {
            Ok(value) => Ok(value),
            Err(error) => match note {
                Some(note) => bail!("{error:#}; {note}; SSH stderr: {}", stderr.trim()),
                None if status.success() => Err(error),
                None => bail!("{error:#}; SSH stderr: {}", stderr.trim()),
            },
        }
    }

    pub fn abort(mut self) -> Option<String> {
        drop(self.stdin.take());
        if let Some(stall) = &self.stall {
            stall.disarm();
        }
        if let Err(error) = kill_child(&self.child) {
            warn!(%error, "could not terminate SSH command");
        }
        if let Err(error) = wait_child(&self.child) {
            warn!(%error, "could not wait for SSH command");
        }
        let note = self.stall_note();
        let response = join(self.response).ok()?;
        if let Err(error) = join(self.stderr).and_then(|stderr| stderr) {
            warn!(%error, "could not read SSH stderr");
        }
        let error = response.err()?;
        error
            .downcast_ref::<RemoteError>()
            .map(ToString::to_string)
            .or(note)
    }
}

pub struct SshStream {
    child: Arc<Mutex<Child>>,
    output: BufReader<ChildStdout>,
    stderr: Option<JoinHandle<Result<String>>>,
    stall: Arc<Stall>,
}

impl SshStream {
    pub fn spawn(remote: &SshLocation, request: &AgentRequest) -> Result<Self> {
        let mut child = ssh_command(remote)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let stderr = drain_stderr(&mut child)?;
        {
            let mut input = child.stdin.take().context("SSH stdin is unavailable")?;
            write_request(&mut input, request)?;
        }
        let output = child.stdout.take().context("SSH stdout is unavailable")?;
        let child = Arc::new(Mutex::new(child));
        let stall = Stall::arm(&child);
        Ok(Self {
            child,
            output: BufReader::new(output),
            stderr: Some(stderr),
            stall,
        })
    }

    pub fn reader(&mut self) -> ProgressReader<'_> {
        ProgressReader {
            inner: &mut self.output,
            stall: &self.stall,
        }
    }

    pub fn wait(&mut self) -> Result<ExitStatus> {
        let status = wait_child(&self.child)?;
        self.stall.disarm();
        Ok(status)
    }

    pub fn terminate(&mut self) -> Result<()> {
        self.stall.disarm();
        kill_child(&self.child)?;
        wait_child(&self.child)?;
        Ok(())
    }

    pub fn failure_detail(&mut self) -> Result<String> {
        let handle = self.stderr.take().context("SSH stderr was already read")?;
        let stderr = join(handle)??;
        let stderr = stderr.trim();
        if self.stall.fired() {
            return Ok(format!("{}; SSH stderr: {stderr}", self.stall.message()));
        }
        Ok(format!("SSH stderr: {stderr}"))
    }
}

pub struct ProgressReader<'stream> {
    inner: &'stream mut BufReader<ChildStdout>,
    stall: &'stream Arc<Stall>,
}

impl Read for ProgressReader<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.stall.bump();
        Ok(read)
    }
}

impl BufRead for ProgressReader<'_> {
    // Bumped before the read because fill_buf's borrow lasts until the caller is
    // done with the buffer. Resetting the clock as the read starts still times out
    // a read that blocks for the whole timeout.
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.stall.bump();
        self.inner.fill_buf()
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
        self.stall.bump();
    }
}

fn join<T>(handle: JoinHandle<T>) -> Result<T> {
    handle
        .join()
        .map_err(|_| anyhow!("SSH reader thread panicked"))
}

pub fn simple_request<T: DeserializeOwned>(
    remote: &SshLocation,
    request: &AgentRequest,
) -> Result<T> {
    let value = Session::start(remote, request)?.finish()?;
    from_value(value).context("decode agent response")
}

fn ssh_command(remote: &SshLocation) -> Command {
    let mut command = Command::new("ssh");
    command.args([
        "-T",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=15",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
    ]);
    if let Some(port) = remote.port {
        command.arg("-p").arg(port.to_string());
    }
    // The separator keeps a hostile target string from being read as an ssh option.
    command
        .arg("--")
        .arg(remote.target())
        .args(["backup", "agent"]);
    command
}

fn write_request(writer: &mut dyn Write, request: &AgentRequest) -> Result<()> {
    to_writer(&mut *writer, request)?;
    writeln!(writer)?;
    writer.flush()?;
    Ok(())
}

pub fn read_response<T: DeserializeOwned>(reader: &mut dyn BufRead) -> Result<T> {
    let mut line = String::new();
    let mut bytes = 0;
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            bail!("agent closed the connection without a response");
        }
        bytes += read;
        if bytes > 64 * 1024 {
            bail!("agent response prefix was not found within 64 KiB");
        }
        if let Some(json) = line.strip_prefix(RESPONSE_PREFIX) {
            let envelope: ResponseEnvelope = from_str(json)?;
            if envelope.protocol == 0 {
                bail!(
                    "the remote backup agent did not report a protocol version; update the backup binary on the remote host"
                );
            }
            if envelope.protocol != PROTOCOL_VERSION {
                bail!(
                    "remote backup protocol {} is incompatible with local protocol {PROTOCOL_VERSION}",
                    envelope.protocol
                );
            }
            if !envelope.ok {
                return Err(Error::new(RemoteError(
                    envelope.error.unwrap_or_else(|| "unknown error".to_owned()),
                )));
            }
            return from_value(envelope.data).context("decode agent response");
        }
    }
}

fn drain_stderr(child: &mut Child) -> Result<JoinHandle<Result<String>>> {
    let mut stderr = child.stderr.take().context("SSH stderr is unavailable")?;
    Ok(thread::spawn(move || {
        let mut message = String::new();
        stderr.read_to_string(&mut message)?;
        Ok(message)
    }))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use serde_json::{Value, to_string};

    use super::read_response;
    use crate::protocol::{PROTOCOL_VERSION, RESPONSE_PREFIX, ResponseEnvelope};

    fn line(envelope: &ResponseEnvelope) -> Cursor<String> {
        Cursor::new(format!(
            "{RESPONSE_PREFIX}{}\n",
            to_string(envelope).unwrap()
        ))
    }

    #[test]
    fn a_response_without_the_current_protocol_is_rejected() {
        let good = ResponseEnvelope {
            ok: true,
            error: None,
            data: Value::Null,
            protocol: PROTOCOL_VERSION,
        };
        read_response::<()>(&mut line(&good)).unwrap();

        let old = ResponseEnvelope {
            protocol: 0,
            ..good.clone()
        };
        let error = read_response::<()>(&mut line(&old)).unwrap_err();
        assert!(format!("{error:#}").contains("update the backup binary"));

        let future = ResponseEnvelope {
            protocol: PROTOCOL_VERSION + 1,
            ..good
        };
        let error = read_response::<()>(&mut line(&future)).unwrap_err();
        assert!(format!("{error:#}").contains("incompatible"));
    }
}
