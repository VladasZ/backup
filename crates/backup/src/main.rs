use std::process::ExitCode;

fn main() -> ExitCode {
    match backup::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "backup command failed");
            eprintln!("error: {error:#}");
            ExitCode::FAILURE
        }
    }
}
