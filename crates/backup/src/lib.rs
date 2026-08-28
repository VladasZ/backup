pub mod agent;
pub mod app;
pub mod archive;
pub mod cli;
pub mod config;
pub mod daemon;
pub mod destination;
pub mod health;
pub mod location;
pub mod lock;
pub mod logging;
pub mod logs;
pub mod operations;
pub mod output;
pub mod paths;
pub mod protocol;
pub mod retention;
pub mod runner;
pub mod service;
pub mod ssh;
pub mod state;
pub mod storage;
pub mod transport;

use std::process::ExitCode;

pub fn run() -> ExitCode {
    app::run()
}
