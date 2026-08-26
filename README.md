# backup

`backup` is a scheduled backup service for macOS and Linux. It creates
independent timestamped archives from local or SSH sources and delivers each
archive to one or more local or SSH directories.

The configuration is deliberately small: give each job a source, destinations,
UTC cron schedule, optional retention rule, and optional exclusions. There is
nothing else to choose.

## Features

- Per-user macOS LaunchAgent and Linux systemd service.
- Standard five-field cron schedules evaluated in UTC.
- Local and SSH sources and destinations.
- Multiple independent destinations per job.
- Full TAR archives with UTC timestamps and UUIDs.
- LZ4 compressed TAR archives, readable by the standard `lz4` tool.
- Infinite retention by default, with optional count or age retention.
- Durable staging and destination-specific retries.
- Atomic publication and BLAKE3 checksum files.
- Pre/post source catalogs with up to five consistency attempts.
- Symbolic links, hard links, metadata, ownership, and extended attributes.
- Gitignore-style exclusions configured per job.
- Automatic configuration reload.
- Rotating log files.
- Pure Rust. No C or C++ source is compiled into the binary.

## Version 1 scope

Version 1 supports macOS and Linux. It does not support Windows, S3,
encryption, filesystem snapshots, special files, or nested mounts.

Every backup is a complete independent archive. Restoring one archive never
requires an older archive.

## Install

The repository pins its Rust toolchain. Nothing else is needed, since every
dependency is pure Rust and no C source is compiled.

```sh
git clone git@github.com:VladasZ/backup.git
cd backup
cargo build --release

mkdir -p "$HOME/.local/bin"
cp target/release/backup "$HOME/.local/bin/backup"
```

Make sure `$HOME/.local/bin` is on `PATH`. Copy the binary to a stable location
before running `backup install`: the service definition points to the exact
executable used for installation.

Run the service as the user who owns the backups. Root is not required.

## Quick start

Copy the example configuration to the default location.

macOS:

```sh
mkdir -p "$HOME/Library/Application Support/backup"
cp config.example.toml \
  "$HOME/Library/Application Support/backup/config.toml"
```

Linux:

```sh
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/backup"
cp config.example.toml \
  "${XDG_CONFIG_HOME:-$HOME/.config}/backup/config.toml"
```

Edit the paths, validate the configuration and endpoints, then run one job:

```sh
backup validate
backup run documents
backup list documents
```

Install the service and watch its logs:

```sh
backup install
backup logs --follow
```

## Configuration

See [`config.example.toml`](config.example.toml) for a complete multi-job
configuration.

```toml
[[backup]]
name = "documents"
source = "/home/alice/Documents"
destinations = [
  "/mnt/backup/documents",
  "ssh://backup-box/srv/backups/documents",
]
cron = "0 2 * * *"
retention = { count = 30 }
exclude = [
  "*.tmp",
  ".cache/",
]
```

Unknown keys are rejected. Local and SSH paths must be absolute and cannot
contain `..`. A destination cannot equal the source or be inside it.

### Compression

Archives are always LZ4. There is no setting, because measurement did not
support one. LZ4 compresses at over 500 MiB/s and costs about 1 second of CPU
per GiB, so it is close to free on any job. Slower algorithms make smaller
archives on text, but a backup runs unattended every night and the archive
travels to its destinations, so speed and predictable cost matter more.

Archives are written as `.tar.lz4` and can be opened with the standard `lz4`
tool without this program.

### Job fields

Each `[[backup]]` record contains:

- `name`: a unique name using letters, numbers, `.`, `_`, or `-`.
- `source`: one local path or SSH URI.
- `destinations`: one or more local paths or SSH URIs.
- `cron`: a five-field cron expression evaluated in UTC.
- `retention`: optional count or age retention.
- `exclude`: optional gitignore-style patterns.

A source may be one regular file or a directory. The source path itself must not
be a symlink, and a symlink source is rejected at validate and run time.
Symlinks found inside the source are stored as symlinks as usual. Sockets, device
files, FIFOs, and other special files fail the archive attempt.

### SSH locations

SSH locations use an unambiguous URI:

```toml
source = "ssh://server.example.com/etc"
source = "ssh://admin@server.example.com:2222/etc"
destinations = ["ssh://backup-box/srv/backups/server"]
```

The URI path must be absolute. Percent-encode reserved path characters, such
as `%20` for a space or `%23` for `#`.

The service invokes the system `ssh` command with batch mode enabled. It uses
normal SSH configuration, aliases, agents, keys, users, ports, and
`IdentityFile` entries. Password prompts are not supported.

The same `backup` binary must be installed on every remote host and available
as `backup` in its non-interactive SSH `PATH`. The controller invokes the
hidden `backup agent` command. A remote host does not need a configuration just
to act as an agent.

Test the remote setup before installing the service:

```sh
ssh -T -o BatchMode=yes backup-box backup --version
backup validate
```

Validation checks the remote protocol, source access, destination directories,
and write access. Missing destination directories are created.

### Scheduling

Cron fields are minute, hour, day of month, month, and day of week:

```toml
cron = "0 2 * * *"   # daily at 02:00 UTC
cron = "30 3 * * 1"  # Monday at 03:30 UTC
cron = "0 */6 * * *" # every six hours
```

The daemon stores the latest handled slot in its state database. On startup it immediately
runs one catch-up backup when the latest slot was missed. The first daemon
start therefore schedules every configured job once.

All jobs and deliveries use one serial queue. Several missed or overlapping
slots collapse into one catch-up run.

A slot is only marked handled after the backup for it succeeds. If a scheduled
backup fails, for example the source is briefly unreachable, the daemon retries
the same slot with growing delays of 1 minute, 5 minutes, 15 minutes, then every
hour, until it works or a newer slot arrives. A newer slot cancels the retry of
an older one, since the catch-up run covers it.

The configuration reloads automatically. An invalid reload is logged and the
last valid configuration remains active.

### Retention

Retention is per job and is applied separately at each destination after a
successful delivery.

```toml
retention = { count = 30 }
```

```toml
retention = { age = "90d" }
```

Age values use durations such as `12h`, `7d`, or `6w`. Count and age are
mutually exclusive. Omit `retention` to keep archives forever.

### Exclusions

```toml
exclude = [
  "*.tmp",
  "*.swp",
  ".cache/",
  "node_modules/",
  "build/**",
]
```

Only patterns in the configuration are used. Source-tree `.gitignore` files
are not loaded automatically.

## Command reference

The global `--config PATH` option may appear before or after a subcommand.

### Validate configuration and endpoints

```sh
backup validate
backup --config /absolute/config.toml validate
```

Validation parses all settings, checks schedules and retention, checks source
access, connects to SSH agents, creates destination directories when needed,
and uses a temporary file to test destination write access.

### Run one job now

```sh
backup run documents
```

Every destination is attempted. A failed destination leaves the archive in
staging for retry.

### Show pending deliveries

```sh
backup status
```

Status lists archives with undelivered destinations. It does not show the
operating system service status.

```sh
launchctl print "gui/$(id -u)/com.vladas.backup" # macOS
systemctl --user status backup.service           # Linux
```

### Read logs

```sh
backup logs
backup logs --follow
```

Logs are printed oldest first. Follow mode continues across rotation.

### List archives

```sh
backup list documents
```

Output includes timestamp, byte size, destination, and archive name. An
unavailable destination is logged without hiding archives at healthy
destinations.

### Restore

Restore the latest archive locally:

```sh
backup restore documents --to /tmp/restored-documents
```

Restore an exact archive:

```sh
backup restore documents \
  documents-2026-07-17T02:00:00Z-01234567-89ab-cdef-0123-456789abcdef.tar.lz4 \
  --to /srv/documents
```

Restore to SSH:

```sh
backup restore documents \
  --to ssh://server.example.com/srv/documents
```

Restore always asks for confirmation before it writes, whether or not the
target already holds files. Use `--yes` for unattended runs:

```sh
backup restore documents --to /srv/documents --yes
```

The checksum and complete archive stream are verified before extraction. If a
copy has no checksum file, restore asks before using it and still reads the
whole archive to confirm it is not truncated, and `--yes` accepts this. If one
destination has a corrupt copy, restore tries another configured destination
with the same archive. Existing archive paths are overwritten, but unrelated
files already in the target are not deleted. Ownership is restored only when
running as root, since only root may change a file's owner.

### Verify

```sh
backup verify
backup verify documents
backup verify documents --archive ARCHIVE_NAME
```

Verification checks BLAKE3 and reads every TAR entry through the selected
decompressor, locally or through the remote agent.

### Apply retention now

```sh
backup prune
backup prune documents
```

Prune applies current retention to every job or only the selected job. Jobs
without retention remain unchanged.

### Run the daemon in the foreground

```sh
backup daemon
```

SIGINT and SIGTERM stop new work and let the current backup or delivery finish.

### Install or remove the service

```sh
backup install
backup uninstall
```

Install runs the same checks as `backup validate` first, so every SSH remote
must be reachable and every source and destination must be valid at install
time. Fix any reported problem before the service is written.

macOS uses:

```text
~/Library/LaunchAgents/com.vladas.backup.plist
```

Linux uses:

```text
~/.config/systemd/user/backup.service
```

The service runs as the installing user. Uninstall does not remove
configuration, state, staged data, logs, or destination archives.

## Archive and delivery behavior

Names use this form:

```text
<job>-<UTC RFC3339 timestamp>-<UUID>.<archive extension>
```

For example:

```text
documents-2026-07-17T02:00:00Z-01234567-89ab-cdef-0123-456789abcdef.tar.lz4
```

Every archive has a neighboring `.blake3` checksum file. Archives and
checksum files are written under partial names, synced, verified, and atomically
renamed into place. The checksum file lets the tool detect a damaged archive
cheaply without unpacking it, both when a copy arrives and later on demand with
`backup verify`.

Deleting old archives during retention does not read checksum files. If an
archive ever loses its checksum file, retention, `list`, and delivery keep
working. `list` marks such an archive, `verify` reports it and exits with an
error, and `restore` asks before restoring it without a checksum, still reading
the whole archive to confirm it is not truncated.

Destinations are independent. Failed destinations retry indefinitely:

- First retry after 1 minute.
- Second retry after 5 minutes.
- Third retry after 15 minutes.
- Later retries every hour.

Successful destinations are not sent the same archive again. A corrupt
destination copy is replaced from staging, and a bad checksum file is
repaired after the archive itself is verified.

Staging is deleted only after all destinations finish. Removing a job from the
configuration does not cancel its recorded pending deliveries.

On startup, complete staged archives not yet recorded in the state database are verified
and recovered for matching active jobs. Corrupt staged archives are preserved
and logged for inspection.

## Consistency and filesystem behavior

Version 1 does not create snapshots. It catalogs source metadata immediately
before and after writing an archive. If the catalog changes, the partial
archive is discarded and retried. After five unstable attempts the job fails
and the error is logged.

This catches normal changes to paths, types, sizes, timestamps, ownership,
links, and extended attributes. It cannot guarantee a perfectly atomic view
of a live filesystem. An application could change contents and restore the
same size and timestamps during the archive window.

For stronger consistency:

- Schedule backups during quiet periods.
- Pause applications while their files are archived.
- Use application-native database dumps and back up the dump directory.
- Restore and verify important archives regularly.

Nested mounts below a source are always skipped and logged. There is no version
1 override.

Symbolic links are stored without following them. Hard links stay hard links.
Extended attributes use PAX headers. Ownership is restored only when running as
root, since only root may change a file's owner. An unprivileged restore keeps
its own ownership and does not fail on that.

## Disk space

Sources, staging, and destinations log a warning at 80 percent use. Use is
measured as space not available to the service, so space a filesystem reserves
for root counts as used, and the warning can appear a little earlier than a
plain disk tool would show.

New archives pause when staging has less free space than the larger of:

- 10 GiB.
- 5 percent of the staging filesystem.

Already staged deliveries remain recorded for retry.

## Runtime files

macOS:

```text
Configuration:
  ~/Library/Application Support/backup/config.toml

State and staging:
  ~/Library/Application Support/backup/

Logs:
  ~/Library/Logs/backup/backup.log
```

Linux:

```text
Configuration:
  $XDG_CONFIG_HOME/backup/config.toml when XDG_CONFIG_HOME is set
  ~/.config/backup/config.toml otherwise

State and staging:
  $XDG_STATE_HOME/backup/ when XDG_STATE_HOME is set
  ~/.local/state/backup/ otherwise

Logs:
  <state directory>/logs/backup.log
```

Logs rotate at 10 MiB and keep nine rotated files plus the current file.
`RUST_LOG` controls the log filter; the default is `info`. The staging path is
not configurable.

## Development

```sh
make ci     # typos, fmt, clippy with -D warnings, unused dependency check
make test   # unit and integration tests, debug and release
make build  # release binary
```

`make ci` needs `typos-cli` and `cargo-machete`. GitHub Actions runs both
targets on Linux for every push and pull request.

The integration tests under `crates/backup/tests/` drive the real binary. The
SSH tests start a container running sshd and exercise every combination of
local and SSH source and destination, so they need Docker. They print a skip
line and pass when Docker is not running.

## Troubleshooting

### SSH validation fails

```sh
ssh -T -o BatchMode=yes backup-box backup --version
```

Check that key or agent authentication works without prompts, the service user
has the expected SSH configuration, `backup` is on the remote non-interactive
`PATH`, and the remote user can access the configured path.

### A backup remains pending

```sh
backup status
backup logs --follow
```

The archive stays in staging while failed destinations retry. Repair the
destination or SSH access and leave the daemon running.

### Source consistency fails five times

Move the schedule to a quieter period, pause the writer, or back up an
application-generated export.

### A configuration edit does not load

```sh
backup validate
```

An active daemon keeps its previous valid configuration after an invalid
reload. A fresh daemon start requires a valid configuration.

### Restore does not preserve ownership

Only root may change a file's owner, so an unprivileged restore skips ownership
and the restored files belong to the user running the restore. Run the restore
as root to keep the original owners, or adjust ownership afterward.

## Development

```sh
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo build --workspace --release
```

The main crate is in `crates/backup`.
