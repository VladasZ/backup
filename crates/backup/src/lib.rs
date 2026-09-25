pub mod agent;
pub mod app;
pub mod archive;
pub mod chunking;
pub mod cli;
pub mod config;
pub mod daemon;
pub mod destination;
pub mod health;
pub mod location;
pub mod lock;
pub mod logging;
pub mod logs;
pub mod maintenance;
pub mod operations;
pub mod output;
pub mod paths;
pub mod pre;
pub mod protocol;
pub mod repair;
pub mod retention;
pub mod runner;
pub mod service;
pub mod ssh;
pub mod state;
pub mod store;
pub mod stream;
pub mod transfer;

use std::process::ExitCode;

pub fn run() -> ExitCode {
    app::run()
}
