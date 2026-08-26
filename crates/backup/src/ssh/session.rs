use std::fmt::{Display, Formatter, Result as FmtResult};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread::{self, JoinHandle};

use anyhow::{Context, Error, Result, anyhow, bail};
use serde::de::DeserializeOwned;
use serde_json::{Value, from_str, from_value, to_writer};

use crate::location::SshLocation;
use crate::protocol::{AgentRequest, RESPONSE_PREFIX, ResponseEnvelope};

#[derive(Debug)]
pub struct RemoteError(pub String);

impl Display for RemoteError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        write!(formatter, "remote agent error: {}", self.0)
    }
}

impl std::error::Error for RemoteError {}

pub struct Session {
    child: Child,
    stdin: Option<ChildStdin>,
    response: JoinHandle<Result<Value>>,
    stderr: JoinHandle<Result<String>>,
}

impl Session {
    pub fn start(remote: &SshLocation, request: &AgentRequest) -> Result<Self> {
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
        let mut session = Self {
            child,
            stdin: None,
            response,
            stderr,
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

    pub fn finish(mut self) -> Result<Value> {
        drop(self.stdin.take());
        let status = self.child.wait()?;
        let response = join(self.response)?;
        let stderr = join(self.stderr)??;
        match response {
            Ok(value) => Ok(value),
            Err(error) if status.success() => Err(error),
            Err(error) => bail!("{error:#}; SSH stderr: {}", stderr.trim()),
        }
    }

    pub fn abort(mut self) -> Option<String> {
        drop(self.stdin.take());
        match self.child.kill() {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::InvalidInput => {}
            Err(error) => tracing::warn!(%error, "could not terminate SSH command"),
        }
        if let Err(error) = self.child.wait() {
            tracing::warn!(%error, "could not wait for SSH command");
        }
        let response = join(self.response).ok()?;
        if let Err(error) = join(self.stderr).and_then(|stderr| stderr) {
            tracing::warn!(%error, "could not read SSH stderr");
        }
        let error = response.err()?;
        error.downcast_ref::<RemoteError>().map(ToString::to_string)
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

pub fn spawn_stream(
    remote: &SshLocation,
    request: &AgentRequest,
) -> Result<(Child, BufReader<ChildStdout>, JoinHandle<Result<String>>)> {
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
    Ok((child, BufReader::new(output), stderr))
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
    command.arg(remote.target()).args(["backup", "agent"]);
    command
}

pub fn write_request(writer: &mut dyn Write, request: &AgentRequest) -> Result<()> {
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
            if !envelope.ok {
                return Err(Error::new(RemoteError(
                    envelope.error.unwrap_or_else(|| "unknown error".to_owned()),
                )));
            }
            return from_value(envelope.data).context("decode agent response");
        }
    }
}

pub fn drain_stderr(child: &mut Child) -> Result<JoinHandle<Result<String>>> {
    let mut stderr = child.stderr.take().context("SSH stderr is unavailable")?;
    Ok(thread::spawn(move || {
        let mut message = String::new();
        stderr.read_to_string(&mut message)?;
        Ok(message)
    }))
}

pub fn join_stderr(handle: JoinHandle<Result<String>>) -> Result<String> {
    join(handle)?
}

pub fn terminate(child: &mut Child) -> Result<()> {
    match child.kill() {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::InvalidInput => {}
        Err(error) => return Err(error).context("terminate SSH command"),
    }
    child.wait().context("wait for terminated SSH command")?;
    Ok(())
}
