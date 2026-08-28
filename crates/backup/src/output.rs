use std::sync::OnceLock;

use anyhow::{Error, Result};
use serde::Serialize;
use serde_json::to_string;
use tracing::warn;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Format {
    Text,
    Json,
}

static FORMAT: OnceLock<Format> = OnceLock::new();

pub fn set_format(format: Format) {
    if FORMAT.set(format).is_err() {
        warn!("output format was already chosen");
    }
}

pub fn format() -> Format {
    FORMAT.get().copied().unwrap_or(Format::Text)
}

// One line per event on stdout in JSON mode. In text mode only the events that
// were user-facing lines before the JSON API keep printing; the rest stay in the
// logs on stderr.
#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Event {
    BackupStarted {
        job: String,
        archive: String,
    },
    Progress {
        bytes: u64,
    },
    DestinationCompleted {
        destination: String,
    },
    DestinationFailed {
        destination: String,
        error: String,
    },
    Verified {
        archive: String,
        destination: String,
    },
    VerifyFailed {
        archive: String,
        destination: String,
        error: String,
    },
    RestoreCopyRejected {
        destination: String,
        error: String,
    },
    Restored {
        archive: String,
        target: String,
    },
}

impl Event {
    fn text(&self) -> Option<String> {
        match self {
            Self::Verified {
                archive,
                destination,
            } => Some(format!("verified {archive} at {destination}")),
            Self::Restored { archive, target } => Some(format!("restored {archive} to {target}")),
            _ => None,
        }
    }
}

pub fn emit(event: &Event) {
    match format() {
        Format::Json => match to_string(event) {
            Ok(line) => println!("{line}"),
            Err(error) => warn!(%error, "could not encode an event as JSON"),
        },
        Format::Text => {
            if let Some(line) = event.text() {
                println!("{line}");
            }
        }
    }
}

#[derive(Serialize)]
struct Envelope<'data, T: Serialize> {
    ok: bool,
    error: Option<String>,
    data: Option<&'data T>,
}

pub fn emit_result<T: Serialize>(data: &T) -> Result<()> {
    let envelope = Envelope {
        ok: true,
        error: None,
        data: Some(data),
    };
    println!("{}", to_string(&envelope)?);
    Ok(())
}

pub fn emit_failure(error: &Error) {
    let envelope: Envelope<'_, ()> = Envelope {
        ok: false,
        error: Some(format!("{error:#}")),
        data: None,
    };
    match to_string(&envelope) {
        Ok(line) => println!("{line}"),
        Err(encode_error) => {
            warn!(%encode_error, "could not encode the error as JSON");
            eprintln!("error: {error:#}");
        }
    }
}
