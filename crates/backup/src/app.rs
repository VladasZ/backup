use anyhow::Result;
use clap::Parser;

use crate::agent;
use crate::cli::{Cli, Command};
use crate::config::Config;
use crate::daemon;
use crate::lock::AppLock;
use crate::logging;
use crate::logs;
use crate::operations;
use crate::paths::AppPaths;
use crate::runner::Runner;
use crate::service;

pub fn run() -> Result<()> {
    let cli = Cli::parse();
    let paths = AppPaths::discover(cli.config)?;
    paths.ensure()?;
    let log_file = match cli.command {
        Command::Daemon | Command::Agent => Some(paths.log_file.as_path()),
        _ => None,
    };
    logging::initialize(log_file)?;
    match cli.command {
        Command::Agent => agent::run(&paths),
        Command::Validate => {
            let config = Config::load(&paths.config)?;
            operations::validate(&config)?;
            println!("configuration is valid: {} job(s)", config.jobs.len());
            Ok(())
        }
        Command::Daemon => daemon::run(Config::load(&paths.config)?, paths),
        Command::Run { job } => {
            let mut runner = Runner::new(Config::load(&paths.config)?, paths)?;
            runner.run_named(&job)?;
            println!("backup {job} completed or staged for retry");
            Ok(())
        }
        Command::Status => {
            let runner = Runner::new(Config::load(&paths.config)?, paths)?;
            runner.print_status()
        }
        Command::Logs { follow } => logs::show(&paths.log_file, follow),
        Command::List { job } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::shared(&paths.operation_lock)?;
            let result = operations::list(config.job(&job)?);
            drop(operation_lock);
            result
        }
        Command::Restore {
            job,
            archive,
            to,
            yes,
        } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::exclusive(&paths.operation_lock)?;
            let result = operations::restore(config.job(&job)?, &archive, &to, yes, &paths);
            drop(operation_lock);
            result
        }
        Command::Verify { job, archive } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::shared(&paths.operation_lock)?;
            let result = operations::verify(&config, job.as_deref(), archive.as_deref());
            drop(operation_lock);
            result
        }
        Command::Prune { job } => {
            let config = Config::load(&paths.config)?;
            let operation_lock = AppLock::exclusive(&paths.operation_lock)?;
            let result = operations::prune(&config, job.as_deref());
            drop(operation_lock);
            result
        }
        Command::History { job } => {
            let runner = Runner::new(Config::load(&paths.config)?, paths)?;
            runner.print_history(job.as_deref())
        }
        Command::Install => {
            let config = Config::load(&paths.config)?;
            operations::validate(&config)?;
            service::install(&paths.config)
        }
        Command::Uninstall => service::uninstall(),
    }
}
