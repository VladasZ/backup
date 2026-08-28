use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use chrono::Utc;
use clap::Parser;
use serde::Serialize;
use tracing::error;

use crate::agent;
use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::daemon;
use crate::health::{self, HealthReport};
use crate::lock::AppLock;
use crate::logging;
use crate::logs;
use crate::operations::{self, ArchiveEntry};
use crate::output::{self, Format};
use crate::paths::AppPaths;
use crate::runner::{HISTORY_DAYS, Runner};
use crate::service;
use crate::state::{HistoryLine, StatusLine};

pub fn run() -> ExitCode {
    let cli = Cli::parse();
    let json = cli.json;
    match execute(cli) {
        Ok(code) => code,
        Err(failure) => {
            if json {
                output::emit_failure(&failure);
            } else {
                error!(error = %failure, "backup command failed");
                eprintln!("error: {failure:#}");
            }
            ExitCode::FAILURE
        }
    }
}

fn execute(cli: Cli) -> Result<ExitCode> {
    let paths = AppPaths::discover(cli.config)?;
    paths.ensure()?;
    let log_file = match cli.command {
        Command::Daemon | Command::Agent => Some(paths.log_file.as_path()),
        _ => None,
    };
    logging::initialize(log_file)?;
    if cli.json {
        ensure_json_supported(&cli.command)?;
        output::set_format(Format::Json);
    }
    match cli.command {
        Command::Agent => {
            agent::run(&paths)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Validate => {
            let config = Config::load(&paths.config)?;
            operations::validate(&config)?;
            let report = ValidateReport {
                jobs: config.jobs.len(),
            };
            match output::format() {
                Format::Json => output::emit_result(&report)?,
                Format::Text => println!("configuration is valid: {} job(s)", report.jobs),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Daemon => {
            daemon::run(Config::load(&paths.config)?, paths)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Run { job } => {
            let mut runner = Runner::new(Config::load(&paths.config)?, paths)?;
            let report = runner.run_named(&job)?;
            match output::format() {
                Format::Json => output::emit_result(&report)?,
                Format::Text => println!("backup {job} completed or staged for retry"),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Status => {
            let runner = Runner::new(Config::load(&paths.config)?, paths)?;
            let lines = runner.state.status()?;
            match output::format() {
                Format::Json => output::emit_result(&lines)?,
                Format::Text => print_status(&lines),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Logs { follow } => {
            logs::show(&paths.log_file, follow)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::List { job } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::shared(&paths.operation_lock)?;
            let result = operations::list(&config, job.as_deref());
            drop(operation_lock);
            let archives = result?;
            match output::format() {
                Format::Json => output::emit_result(&archives)?,
                Format::Text => print_list(job.as_deref(), &archives),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Restore {
            job,
            archive,
            to,
            yes,
        } => {
            if output::format() == Format::Json && !yes {
                bail!("restore with --json needs --yes, prompts cannot be answered in JSON mode");
            }
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::exclusive(&paths.operation_lock)?;
            let result = operations::restore(config.job(&job)?, &archive, &to, yes, &paths);
            drop(operation_lock);
            let restored = result?;
            if output::format() == Format::Json {
                let report = restored.context("restore was cancelled")?;
                output::emit_result(&report)?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Verify { job, archive } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::shared(&paths.operation_lock)?;
            let result = operations::verify(&config, job.as_deref(), archive.as_deref());
            drop(operation_lock);
            let verified = result?;
            if output::format() == Format::Json {
                output::emit_result(&VerifyReport { verified })?;
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Prune { job } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::exclusive(&paths.operation_lock)?;
            let result = operations::prune(&config, job.as_deref());
            drop(operation_lock);
            let pruned = result?;
            match output::format() {
                Format::Json => output::emit_result(&pruned)?,
                Format::Text => {
                    for entry in &pruned {
                        println!("pruned {} at {}", entry.job, entry.destination);
                    }
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::History { job } => {
            let runner = Runner::new(Config::load(&paths.config)?, paths)?;
            let lines: Vec<_> = runner
                .state
                .history()?
                .into_iter()
                .filter(|line| job.as_deref().is_none_or(|job| line.job == job))
                .collect();
            match output::format() {
                Format::Json => output::emit_result(&lines)?,
                Format::Text => print_history(&lines),
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Forget { job } => {
            let mut runner = Runner::new(Config::load(&paths.config)?, paths)?;
            let in_config = runner.config.jobs.iter().any(|item| item.name == job);
            let cancelled = runner.forget(&job, !in_config)?;
            let report = ForgetReport {
                job: job.clone(),
                cancelled,
            };
            match output::format() {
                Format::Json => output::emit_result(&report)?,
                Format::Text => {
                    if report.cancelled.is_empty() {
                        println!("no pending deliveries for {job}");
                    }
                    for archive in &report.cancelled {
                        println!("cancelled pending deliveries for {archive}");
                    }
                }
            }
            Ok(ExitCode::SUCCESS)
        }
        Command::Health => {
            let runner = Runner::new(Config::load(&paths.config)?, paths)?;
            let report = health::check(
                &runner.config,
                &runner.state,
                &runner.paths.daemon_lock,
                &runner.paths.operation_lock,
                Utc::now(),
            )?;
            match output::format() {
                Format::Json => output::emit_result(&report)?,
                Format::Text => print_health(&report),
            }
            Ok(if report.healthy {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            })
        }
        Command::Install => {
            let config = Config::load(&paths.config)?;
            operations::validate(&config)?;
            service::install(&paths.config)?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Uninstall => {
            service::uninstall()?;
            Ok(ExitCode::SUCCESS)
        }
    }
}

fn ensure_json_supported(command: &Command) -> Result<()> {
    match command {
        Command::Daemon
        | Command::Agent
        | Command::Logs { .. }
        | Command::Install
        | Command::Uninstall => {
            bail!("--json is not supported for this command")
        }
        _ => Ok(()),
    }
}

#[derive(Serialize)]
struct ValidateReport {
    jobs: usize,
}

#[derive(Serialize)]
struct VerifyReport {
    verified: usize,
}

#[derive(Serialize)]
struct ForgetReport {
    job: String,
    cancelled: Vec<String>,
}

fn print_status(lines: &[StatusLine]) {
    if lines.is_empty() {
        println!("no pending deliveries");
        return;
    }
    for line in lines {
        println!(
            "{}  {}  {}  {} destination(s) pending",
            line.created_at.to_rfc3339(),
            line.job,
            line.archive,
            line.pending_destinations
        );
    }
}

fn print_history(lines: &[HistoryLine]) {
    if lines.is_empty() {
        println!("no completed backups in the last {HISTORY_DAYS} days");
        return;
    }
    for line in lines {
        println!(
            "{}  {}  {:>12}  {}  finished {}",
            line.created_at.to_rfc3339(),
            line.job,
            line.size,
            line.archive,
            line.completed_at.to_rfc3339()
        );
    }
}

fn print_list(job: Option<&str>, archives: &[ArchiveEntry]) {
    if archives.is_empty() {
        match job {
            Some(job) => println!("no archives found for {job}"),
            None => println!("no archives found"),
        }
        return;
    }
    for archive in archives {
        let note = if archive.checksum_missing {
            "  (checksum file missing)"
        } else {
            ""
        };
        println!(
            "{}  {:>12}  {}  {}{note}",
            archive.created.to_rfc3339(),
            archive.size,
            archive.destination,
            archive.name
        );
    }
}

fn print_health(report: &HealthReport) {
    println!(
        "daemon: {}",
        if report.daemon_running {
            "running"
        } else {
            "not running"
        }
    );
    if report.busy {
        println!("a backup, delivery, or restore is running");
    }
    for job in &report.jobs {
        if job.healthy {
            println!("{}: ok", job.name);
            continue;
        }
        for problem in &job.problems {
            println!("{}: {}", job.name, problem);
        }
    }
    println!(
        "{}",
        if report.healthy {
            "healthy"
        } else {
            "unhealthy"
        }
    );
}
