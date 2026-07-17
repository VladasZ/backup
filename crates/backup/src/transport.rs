use anyhow::Result;

use crate::archive::Artifact;
use crate::config::BackupJob;
use crate::destination::deliver_local;
use crate::location::Location;
use crate::ssh::deliver_remote;

pub fn deliver(artifact: &Artifact, destination: &Location, job: &BackupJob) -> Result<()> {
    match destination {
        Location::Local(path) => deliver_local(artifact, path, job),
        Location::Ssh(remote) => deliver_remote(artifact, remote, job),
    }
}
