use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::location::Location;

#[derive(Debug, Parser)]
#[command(version, about = "Reliable scheduled backups to local and SSH folders")]
pub struct Cli {
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    #[arg(long, global = true)]
    pub json: bool,

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
        job: Option<String>,
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
    Health,
    Install,
    Uninstall,
    #[command(hide = true)]
    Agent,
}
