use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::location::Location;

#[derive(Debug, Parser)]
#[command(version, about = "Reliable scheduled backups to local and SSH folders")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Daemon,
    Validate,
    Run {
        job: String,
    },
    Status,
    Logs {
        #[arg(short, long)]
        follow: bool,
    },
    List {
        job: String,
    },
    Restore {
        job: String,

        #[arg(default_value = "latest")]
        archive: String,

        #[arg(long)]
        to: Location,

        #[arg(long)]
        yes: bool,
    },
    Verify {
        job: Option<String>,

        #[arg(long)]
        archive: Option<String>,
    },
    Prune {
        job: Option<String>,
    },
    History {
        job: Option<String>,
    },
    Install,
    Uninstall,
    #[command(hide = true)]
    Agent,
}
