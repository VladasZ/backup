# backup

`backup` is a scheduled backup service for macOS and Linux. It backs up local
or SSH sources into one or more local or SSH directories. Each directory is a
deduplicated store: data that did not change since the last backup is not
stored again.

The configuration is deliberately small: give each job a source, destinations,
UTC cron schedule, optional retention rule, optional pre command, and optional
exclusions. There is nothing else to choose.

## Features

- Per-user macOS LaunchAgent and Linux systemd service.
- Standard five-field cron schedules evaluated in UTC.
- Local and SSH sources and destinations.
- Multiple independent destinations per job.
- Every backup is a full TAR stream with a UTC timestamp and UUID.
- Content-defined deduplication with FastCDC, so unchanged data is stored once.
- Chunks named by their BLAKE3 hash and compressed with LZ4.
- Reed-Solomon parity on every stored file, so small damage heals in place.
- A weekly full verify that repairs damage from the other destinations.
- Export of any backup as a plain `.tar.lz4` that opens without this tool.
- Infinite retention by default, with optional count or age retention.
- Milestone archives of week, month, and year ages that are always kept.
- Streaming delivery to every destination at once, only chunks it lacks.
- A failed destination is filled later from one that succeeded.
- Atomic publication of every stored file.
- Pre/post source catalogs that report files changed during the archive pass.
- Symbolic links, hard links, metadata, ownership, and extended attributes.
- Sockets, FIFOs, device files, and unreadable entries are skipped with a warning.
- Thirty days of completed run history.
- A health command for monitoring and a JSON mode for GUI integrations.
- Gitignore-style exclusions configured per job.
- An optional command run before each archive, for database dumps.
- Automatic configuration reload.
- Rotating log files.
- Automatic import of archives from the old one-file-per-backup format.
- Pure Rust. No C or C++ source is compiled into the binary.

## Version 1 scope

Version 1 supports macOS and Linux. It does not support Windows, S3,
encryption, filesystem snapshots, special files, or nested mounts.

Every backup restores on its own. Backups share stored chunks, but no backup
is stored as a difference against an older one.

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

### Static Linux binary

On Linux, build a statically linked binary instead:

```sh
make static
```

The result is at `target/x86_64-unknown-linux-musl/release/backup`. It carries
its own C library, so it runs on any x86_64 Linux host whatever distribution it
uses. Prefer this build when the controller reaches other hosts over SSH. A
normal build records the paths of the libraries it needs, and those paths may
not exist on the host that receives the copied agent, so the copy would not
start there.

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

A configuration with no jobs is valid. The daemon starts, idles, and picks up
jobs when they are added. This matters when another program writes the file and
has a normal state with nothing scheduled yet.

### Compression

Stored chunks are always LZ4. There is no setting, because measurement did not
support one. LZ4 compresses at over 500 MiB/s and costs about 1 second of CPU
per GiB, so it is close to free on any job. Slower algorithms make smaller
archives on text, but a backup runs unattended every night and the archive
travels to its destinations, so speed and predictable cost matter more.

`backup export` writes any backup as a `.tar.lz4` file that the standard `lz4`
and `tar` tools open without this program.

### Job fields

Each `[[backup]]` record contains:

- `name`: a unique name using letters, numbers, `.`, `_`, or `-`.
- `source`: one local path or SSH URI.
- `destinations`: one or more local paths or SSH URIs.
- `cron`: a five-field cron expression evaluated in UTC.
- `retention`: optional count or age retention.
- `pre`: optional shell command run before the archive.
- `exclude`: optional gitignore-style patterns.

A source may be one regular file or a directory. The source path itself must not
be a symlink, and a symlink source is rejected at validate and run time.
Symlinks found inside the source are stored as symlinks as usual. Sockets, device
files, FIFOs, and other special files are skipped and logged. A file or directory
the service cannot read, for example because of permissions, is also skipped and
logged, so one unreadable entry does not fail the whole backup. Only the source
path itself must be readable.

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

The controller runs the hidden `backup agent` command on the remote host. It
does not need one installed there. On first contact it copies its own binary to
`~/.cache/backup/agent-<hash>` on the remote and runs that, so both ends always
run the same build and the protocol versions cannot drift. The name carries a
hash of the binary, so a new build lands next to the old one instead of
replacing a copy that a running session is using. A remote host does not need a
configuration just to act as an agent.

The copied binary is proved by running it, not by comparing platforms, because a
remote can report the same kernel and machine and still not run the binary. When
the copy does not run there, for example a macOS controller and a Linux remote,
the controller falls back to a `backup` binary installed on the remote and
available in its non-interactive SSH `PATH`.

Use the static Linux build for a controller that reaches other Linux hosts, so
the copy always runs and the fallback is never needed. See
[Static Linux binary](#static-linux-binary).

Every agent response carries the protocol version, so a remote running an
incompatible backup binary fails any command with a clear error, not only
`backup validate`.

Test the remote setup before installing the service:

```sh
ssh -T -o BatchMode=yes backup-box true
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

Besides the archives the count or age rule keeps, every job always keeps one
milestone archive per age bucket: 1 to 2 weeks, 2 weeks to 1 month, 1 to 2
months, 2 to 3 months, 3 to 6 months, 6 months to 1 year, and then one per
year forever. The oldest archive in each bucket is kept, so it slides into the
next bucket as it ages and there is always a backup of each approximate age.
Milestone archives are never removed by the count or age rule, and the rule
counts only the remaining archives. There is no setting for this. A month is
30 days and a year is 365 days, measured from the timestamp in the archive
name. Jobs without a retention rule are unaffected, since they never delete
anything.

### Pre command

A job may run one shell command before its archive is made:

```toml
pre = "pg_dump -U app app > /srv/app/dumps/app.sql"
```

The command runs where the source is. A local source runs it on this machine, an
SSH source runs it on the remote host through the agent. It runs with `sh -c`,
before the source is scanned, so anything it writes into the source is part of
the archive.

The command must succeed. A non-zero exit fails the whole run, the error carries
its standard error, and nothing is delivered. A scheduled run that fails this way
is retried like any other failed slot.

The main use is a database. A live database folder cannot be archived file by
file and be trusted to start again, so exclude the folder and archive a dump
instead:

```toml
pre = "pg_dump -U app app > /srv/app/dumps/app.sql"
exclude = ["postgres/"]
```

Output is captured and not shown, so redirect anything you want to keep into a
file inside the source.

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

Every destination is written at once. When at least one destination succeeds,
a failed one is filled later by copying the backup from a destination that has
it. When every destination fails, the run fails. Commands other than `daemon`
print their log lines to stderr.

### Show pending deliveries

```sh
backup status
```

Status lists archives with undelivered destinations. It does not show the
operating system service status.

### Show completed backups

```sh
backup history
backup history documents
```

History lists runs that finished on every destination during the last 30 days,
newest first, with the archive size and finish time. Older runs are removed from
the state database by the daemon once an hour.

```sh
launchctl print "gui/$(id -u)/com.vladas.backup" # macOS
systemctl --user status backup.service           # Linux
```

### Check health

```sh
backup health
```

Health exits with code 0 when everything is fine and 1 otherwise, so a cron
job or monitor can watch it. It reports a problem when the daemon is not
running, when a job's latest scheduled slot has not completed within one hour,
when a delivery has been pending for over one hour, or when a job that is no
longer in the configuration still has pending deliveries. It also reports a
destination that has gone more than 8 days without a verify, damage the last
verify could not repair, an old archive that could not be imported, and a
newest backup that is less than half the size of the one before it. There is no
setting for any of these limits.

While a backup, delivery, restore, verify, or import is running, the grace period is 24 hours
instead of one hour, so a run that takes several hours does not report as a
problem while it is still moving data. The queue is serial, so jobs waiting
behind it are covered by the same rule. The report says when an operation is
running.

Only the daemon marks scheduled slots as handled, so a machine that never runs
the daemon reports its slots as missed. Health is a check on the scheduled
service, not on manual runs.

### Read logs

```sh
backup logs
backup logs --follow
```

Logs are printed oldest first. Follow mode continues across rotation.

### List archives

```sh
backup list
backup list documents
```

Without a job name, archives of every configured job are listed. Output
includes timestamp, byte size, destination, and archive name. An
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
  documents-20260717T020000Z-01234567-89ab-cdef-0123-456789abcdef.tar.lz4 \
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

Every chunk is checked against its hash as it is read, and the whole stream is
checked against the backup's BLAKE3 at the end. When a chunk is damaged in one
destination, restore takes that chunk from another destination that holds the
same backup. Existing archive paths are overwritten, but unrelated
files already in the target are not deleted. Ownership is restored only when
running as root, since only root may change a file's owner.

### Verify

```sh
backup verify
backup verify documents
backup verify documents --archive ARCHIVE_NAME
backup verify documents --archive latest
```

Verification reads everything stored in each destination of the selected jobs
and checks every chunk against its hash. Damage the parity cannot heal is
repaired from the other destinations, then the destination is read again.
Because it repairs, verify waits for any running backup. With `--archive`, the
named backup is also rebuilt from each destination alone and read as a TAR.
`--archive latest` resolves the newest backup per job.

The daemon runs the same verify on every destination once a week, between
scheduled backups.

### Export

```sh
backup export documents --to /tmp/documents.tar.lz4
backup export documents ARCHIVE_NAME --to /tmp/documents.tar.lz4
```

Export rebuilds one backup and writes it as a plain `.tar.lz4`:

```sh
lz4 -dc /tmp/documents.tar.lz4 | tar -x -C /tmp/restored
```

### Cancel pending deliveries

```sh
backup forget documents
```

Forget cancels every pending delivery of one job. Backups already delivered
stay where they are. For a job that is no
longer in the configuration it also clears the stored schedule and retry
state, so nothing is left behind and health stops reporting it.

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

SIGINT and SIGTERM stop new work and let the current backup, delivery, verify,
or import step finish.

### JSON output

Every command except `daemon`, `agent`, `logs`, `install`, and `uninstall`
accepts a global `--json` flag for integration with GUI applications and
scripts. All JSON goes to stdout, one object per line. Log lines stay on
stderr.

The final line of every `--json` command is a result envelope:

```json
{"ok":true,"error":null,"data":{...}}
{"ok":false,"error":"unknown backup job \"missing\"","data":null}
```

Exit codes are unchanged, so `ok` mirrors the exit status. `status`,
`history`, and `list` return arrays in `data`. `health` returns the health
report in `data` and still exits with 1 when unhealthy.

`run`, `verify`, and `restore` stream progress events before the final
envelope, one JSON object per line with an `event` field:

```json
{"event":"backup_started","job":"documents","archive":"documents-...tar.lz4"}
{"event":"progress","bytes":67108864}
{"event":"destination_completed","destination":"/mnt/backup/documents"}
{"event":"verified","archive":"documents-...tar.lz4","destination":"/mnt/backup/documents"}
{"event":"restored","archive":"documents-...tar.lz4","target":"/tmp/restored"}
```

Progress events appear roughly every 64 MiB of archive data. Unknown event
types may be added later, so a consumer should ignore events it does not
recognize. Restore prompts cannot be answered in JSON mode, so
`restore --json` requires `--yes`.

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
configuration, state, logs, or destination data.

## Storage and delivery

A backup is the TAR stream of the source, cut into content-defined chunks with
FastCDC. A cut depends on the bytes around it, not on their offset, so an edit
changes only the chunks next to it. Chunks average 1 MiB, with a minimum of
256 KiB and a maximum of 4 MiB. Each chunk is named by its BLAKE3 hash and
stored once per destination, however many backups use it.

A destination folder holds:

```text
packs/ab/<blake3>.pack        chunks, about 32 MB per pack
index/<blake3>.index          which pack holds which chunk
recipes/<name>.recipe         one per backup, its chunks in order
```

A recipe also holds the length and BLAKE3 of the whole stream. The index is
only a cache. A pack that no index file lists is read and indexed again, so
losing every index file loses nothing. Several jobs may share one destination
and its chunks.

Every pack, index file and recipe is sealed with parity. The file is cut into
64 pieces and Reed-Solomon adds 2 parity pieces, about 3 percent more space.
Each piece has its BLAKE3 in a header that is written at both ends of the file.
Up to 2 damaged pieces per file are rebuilt in place, without another copy.
Every file is written under a hidden partial name, synced, and renamed into
place. The recipe is written last, so a backup exists only once all its chunks
do.

The source is read once per run and the chunks go to every destination at
once. Each destination is sent only the chunks it does not hold. From an SSH
source, only chunks that some destination lacks cross the connection. Every
chunk that arrives over a connection is checked against its hash before it is
stored.

Names use this form:

```text
<job>-<compact UTC timestamp>-<UUID>.tar.lz4
```

For example:

```text
documents-20260717T020000Z-01234567-89ab-cdef-0123-456789abcdef.tar.lz4
```

The name keeps the `.tar.lz4` ending because `export` turns the backup into
exactly that file. The timestamp has no colons on purpose. Colons are illegal
in SMB names, so an RFC3339 name shows up mangled over a samba share.

A destination that fails during a run gets a pending delivery. It retries
indefinitely by copying the backup from a destination that has it:

- First retry after 1 minute.
- Second retry after 5 minutes.
- Third retry after 15 minutes.
- Later retries every hour.

Retention removes recipes, then a cleanup deletes packs that no recipe uses
and rewrites packs whose live share has fallen below 70 percent. Cleanup runs
after every delivery, also for jobs without retention. A chunk stored in more
than one pack counts in only one of them, so extra copies are freed too, but
only after the kept copy was read back intact. Packs younger than one hour are
left alone, since a run may still be writing them.

A remote that accepts the SSH connection but then stops making progress cannot
stall the queue. When no data moves for 15 minutes, the connection is
terminated, that destination is marked failed, and the normal retry schedule
applies. There is no setting for this.

An interrupted write can leave a hidden partial file behind. Partial files
older than one day are deleted automatically. A write that is still running is
never touched, since its partial file keeps a fresh modification time.

Removing a job from the configuration does not cancel its recorded pending
deliveries. Use `backup forget` to cancel them.

Stored files are created readable only by their owner, so other users of a
shared destination cannot read backup contents.

### Old archives

Older versions stored each backup as one `.tar.lz4` file with a `.blake3` file
next to it. The daemon imports these on its own, one at a time, oldest first,
between scheduled backups. Each import reads the file once, checks it against
its `.blake3`, and stores its chunks. The original is deleted only after the
stream rebuilt from the store matches it byte for byte and reads as a valid
TAR. A failed import keeps the original, shows in `backup health`, and is tried
again after a day.

## Consistency and filesystem behavior

Version 1 does not create snapshots. It catalogs source metadata while writing
the archive and again right after. If the catalog changed, the archive is still
published and a warning lists the changed paths, up to twenty of them, with the
total count. A slightly inconsistent backup is better than none, and the log
tells you which files to check.

A file that shrinks while it is being read is padded with zeros in the archive
and a file that grows is cut at its cataloged size, so the archive stays valid.

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

There is no free space check before a run, and no warning about a filesystem
filling up. A destination disk that fills up fails only that destination, and
the other destinations continue.

## Runtime files

macOS:

```text
Configuration:
  ~/Library/Application Support/backup/config.toml

State:
  ~/Library/Application Support/backup/

Logs:
  ~/Library/Logs/backup/backup.log
```

Linux:

```text
Configuration:
  $XDG_CONFIG_HOME/backup/config.toml when XDG_CONFIG_HOME is set
  ~/.config/backup/config.toml otherwise

State:
  $XDG_STATE_HOME/backup/ when XDG_STATE_HOME is set
  ~/.local/state/backup/ otherwise

Logs:
  <state directory>/logs/backup.log
```

Logs rotate at 10 MiB and keep nine rotated files plus the current file. Only
the daemon and the remote agent write to the log file. Other commands log to
stderr. `RUST_LOG` controls the log filter. The default is `info`.

## Development

```sh
make ci     # typos, fmt, clippy with -D warnings, unused dependency check
make test   # unit and integration tests, debug and release
make build  # release binary
make static # statically linked Linux release binary
```

`make ci` needs `typos-cli` and `cargo-machete`. GitHub Actions runs both
targets on Linux for every push and pull request.

The integration tests under `crates/backup/tests/` drive the real binary. The
SSH tests start a container running sshd and exercise every combination of
local and SSH source and destination, so they need Docker. They print a skip
line and pass when Docker is not running. On macOS the Linux agent binary is
built in a container and rebuilt whenever a source file is newer than it.

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

A failed destination retries by copying the backup from another destination.
Repair the destination or SSH access and leave the daemon running.

### The log warns that the source changed

The archive was still published. Check the listed paths, and for important
data move the schedule to a quieter period, pause the writer, or back up an
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
