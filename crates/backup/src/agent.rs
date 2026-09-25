use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write, stdin, stdout};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::Serialize;
use serde_json::{Value, from_str, to_value, to_writer};
use tracing::error;
use uuid::Uuid;

use crate::archive::{
    ChunkEvent, SourceScanner, archive_name, ensure_not_symlink, produce, restore_stream,
};
use crate::config::BackupJob;
use crate::destination::verify_local;
use crate::paths::AppPaths;
use crate::pre;
use crate::protocol::{
    AgentRequest, ChunkCount, FrameReader, PROTOCOL_VERSION, PingResponse, RESPONSE_PREFIX, Record,
    ResponseEnvelope, StreamHeader, StreamTrailer, encode_data, encode_missing, encode_reference,
    read_ids, read_record, write_end_frame, write_frame, write_ids,
};
use crate::store::Store;
use crate::store::check::{check_store, finish_repair, put_repaired};
use crate::store::digest::Digest;
use crate::store::gc::apply_retention;
use crate::store::import::{import_legacy, legacy_names};
use crate::store::recipe::Recipe;

pub fn run(paths: &AppPaths) -> Result<()> {
    paths.ensure()?;
    let input = stdin();
    let mut reader = BufReader::new(input.lock());
    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .context("read agent request")?;
    let request: AgentRequest = from_str(&request_line).context("parse agent request")?;
    if let Err(error) = handle(request, &mut reader) {
        error!(%error, "agent operation failed");
        write_error(&format!("{error:#}"))?;
    }
    Ok(())
}

fn handle(request: AgentRequest, reader: &mut dyn BufRead) -> Result<()> {
    match request {
        AgentRequest::Ping => write_success(&PingResponse {
            protocol: PROTOCOL_VERSION,
            version: env!("CARGO_PKG_VERSION").to_owned(),
        }),
        AgentRequest::ValidateSource { path } => {
            ensure_not_symlink(&path)?;
            let metadata =
                fs::metadata(&path).with_context(|| format!("read source {}", path.display()))?;
            if !metadata.is_dir() && !metadata.is_file() {
                bail!(
                    "source {} is not a regular file or directory",
                    path.display()
                );
            }
            write_success(&Value::Null)
        }
        AgentRequest::ValidateDestination { path } => {
            validate_destination_path(&path)?;
            write_success(&Value::Null)
        }
        AgentRequest::Create {
            job,
            source,
            exclude,
            pre,
        } => create(reader, &job, source, &exclude, pre.as_deref()),
        AgentRequest::Chunks { destination } => {
            let ids = Store::open(&destination)?.chunk_ids()?;
            write_success(&ChunkCount { count: ids.len() })?;
            let mut output = stdout().lock();
            write_ids(&mut output, &ids)?;
            output.flush()?;
            Ok(())
        }
        AgentRequest::Receive { destination, job } => receive(reader, &destination, &job),
        AgentRequest::List { destination, job } => {
            write_success(&Store::open(&destination)?.list_recipes(job.as_deref())?)
        }
        AgentRequest::ReadRecipe { destination, name } => {
            write_success(&Store::open(&destination)?.read_recipe(&name)?)
        }
        AgentRequest::ReadChunks { destination } => read_chunks(reader, &destination),
        AgentRequest::Restore { target } => {
            let mut frames = FrameReader::new(reader);
            restore_stream(&mut frames, &target)?;
            frames.finish()?;
            write_success(&Value::Null)
        }
        AgentRequest::Check { destination } => {
            write_success(&check_store(&mut Store::open(&destination)?)?)
        }
        AgentRequest::Repair {
            destination,
            damaged,
            recipes,
        } => repair(reader, &destination, &damaged, &recipes),
        AgentRequest::VerifyArchive { destination, name } => {
            let mut store = Store::open(&destination)?;
            let recipe = store.read_recipe(&name)?;
            verify_local(&mut store, &recipe)?;
            write_success(&Value::Null)
        }
        AgentRequest::Prune { destination, job } => {
            apply_retention(Store::open(&destination)?, &job)?;
            write_success(&Value::Null)
        }
        AgentRequest::Legacy { destination } => write_success(&legacy_names(&destination)?),
        AgentRequest::Import { destination, name } => {
            let (_, report) = import_legacy(Store::open(&destination)?, &name)?;
            write_success(&report)
        }
    }
}

fn validate_destination_path(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create destination {}", path.display()))?;
    if !fs::metadata(path)?.is_dir() {
        bail!("destination {} is not a directory", path.display());
    }
    let probe = path.join(format!(".backup-write-test-{}", Uuid::new_v4()));
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .with_context(|| format!("destination {} is not writable", path.display()))?;
    file.sync_all()?;
    fs::remove_file(&probe).with_context(|| format!("remove write probe {}", probe.display()))
}

fn create(
    reader: &mut dyn BufRead,
    job: &str,
    source: PathBuf,
    exclude: &[String],
    pre: Option<&str>,
) -> Result<()> {
    let mut known = read_ids(reader)?.into_iter().collect();
    if let Some(command) = pre {
        pre::run(job, command)?;
    }
    let scanner = SourceScanner::new(&source, exclude)?;
    let created_at = Utc::now();
    write_success(&StreamHeader {
        name: archive_name(job, created_at),
        created_at,
    })?;
    let produced = produce(job, &scanner, &mut known, &mut |event| {
        let record = match event {
            ChunkEvent::Data { id, bytes } => encode_data(&id, bytes)?,
            ChunkEvent::Reference { id, size } => encode_reference(&id, size),
        };
        write_frame(&mut stdout().lock(), &record)?;
        Ok(())
    });
    // The end frame goes out even after a failure, so the controller reads a
    // complete frame stream followed by the error line.
    let mut output = stdout().lock();
    write_end_frame(&mut output)?;
    output.flush()?;
    drop(output);
    let produced = produced?;
    write_success(&StreamTrailer {
        checksum: produced.checksum,
        size: produced.size,
        changed: produced.changed,
    })
}

fn receive(reader: &mut dyn BufRead, destination: &Path, job: &BackupJob) -> Result<()> {
    let mut writer = Store::open(destination)?.into_writer();
    {
        let mut frames = FrameReader::new(reader);
        while let Some(record) = read_record(&mut frames)? {
            match record {
                Record::Data { id, bytes } => writer.put(&id, &bytes)?,
                Record::Reference { id, .. } | Record::Missing { id } => {
                    bail!("expected chunk data, got a bare reference to {id}")
                }
            }
        }
    }
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let recipe: Recipe = from_str(&line).context("parse the recipe")?;
    if recipe.job != job.name {
        bail!(
            "recipe {} belongs to job {:?}, not {:?}",
            recipe.name,
            recipe.job,
            job.name
        );
    }
    let mut store = writer.finish()?;
    store.ensure_complete(&recipe)?;
    store.write_recipe(&recipe)?;
    apply_retention(store, job)?;
    write_success(&Value::Null)
}

fn read_chunks(reader: &mut dyn BufRead, destination: &Path) -> Result<()> {
    let ids = read_ids(reader)?;
    let mut store = Store::open(destination)?;
    write_success(&Value::Null)?;
    for id in &ids {
        let record = match store.read_chunk(id) {
            Ok(bytes) => encode_data(id, &bytes)?,
            Err(error) => {
                error!(error = %format!("{error:#}"), "cannot read a requested chunk");
                encode_missing(id)
            }
        };
        write_frame(&mut stdout().lock(), &record)?;
    }
    let mut output = stdout().lock();
    write_end_frame(&mut output)?;
    output.flush()?;
    Ok(())
}

fn repair(
    reader: &mut dyn BufRead,
    destination: &Path,
    damaged: &[Digest],
    recipes: &[Recipe],
) -> Result<()> {
    let mut writer = Store::open(destination)?.into_writer();
    {
        let mut frames = FrameReader::new(reader);
        while let Some(record) = read_record(&mut frames)? {
            match record {
                Record::Data { id, bytes } => put_repaired(&mut writer, &id, &bytes)?,
                Record::Reference { id, .. } | Record::Missing { id } => {
                    bail!("expected a repaired chunk, got a bare reference to {id}")
                }
            }
        }
    }
    finish_repair(writer.finish()?, damaged, recipes)?;
    write_success(&Value::Null)
}

fn write_success<T: Serialize>(data: &T) -> Result<()> {
    write_response(&ResponseEnvelope {
        ok: true,
        error: None,
        data: to_value(data)?,
        protocol: PROTOCOL_VERSION,
    })
}

fn write_error(error: &str) -> Result<()> {
    write_response(&ResponseEnvelope {
        ok: false,
        error: Some(error.to_owned()),
        data: Value::Null,
        protocol: PROTOCOL_VERSION,
    })
}

fn write_response(response: &ResponseEnvelope) -> Result<()> {
    let mut output = stdout().lock();
    write!(output, "{RESPONSE_PREFIX}")?;
    to_writer(&mut output, response)?;
    writeln!(output)?;
    output.flush()?;
    Ok(())
}
