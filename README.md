# tard

A fast, parallel directory archiver, plus a server that queues, runs in
parallel and schedules archive jobs.

```sh
tard ~/photos /backup              # writes /backup/photos.tard
tard -x /backup/photos.tard ~/restore
tard server ~/photos /backup -q ~/music /backup   # queue two jobs, one after the other
```

- **Parallel both ways.** Worker threads read files into preallocated buffers
  while a single writer streams them into the archive. Extraction splits the
  archive across workers that each read their own byte ranges.
- **Memory bounded by `-r`.** Buffers are reserved, not filled, so a large
  `-r` costs nothing until data arrives.
- **Resumable.** `-c` continues an interrupted archive or extraction, picking
  up exactly where it stopped.
- **A job server.** `tard server` queues jobs, runs them side by side within
  RAM and CPU limits, pauses and resumes them, schedules recurring runs and
  shows a live full-screen view.
- **Builds on Linux, macOS and Windows.**

## Install

```sh
cargo install --path .
```

Or build with `cargo build --release` and run `target/release/tard`.

## Archiving and extracting

```
tard [OPTIONS] <INPUT> <OUTPUT_DIR>
```

| Option | Meaning | Default |
|---|---|---|
| `-a`, `--archive` | Archive the `INPUT` directory into `OUTPUT_DIR/<name>.tard` | yes |
| `-x`, `--extract` | Extract the `INPUT` `.tard` file into `OUTPUT_DIR` | |
| `-c`, `--continue` | Resume an interrupted run | off |
| `-r`, `--ram <SIZE>` | RAM for buffers, e.g. `512KB`, `64MB`, `1GB` | `10MB` |
| `-n`, `--num_workers <N>` | Worker threads | `8` |
| `-v`, `--verbose` | Print each file's path and size, plus progress notes | off |
| `-l`, `--log <PATH>` | Where the per-file CSV log goes (see [Logs](#logs)) | `~/.tard/logs` |
| `-h`, `--help` | Show help | |

When you resume:

- **Archives:** `-c` reads the paths already stored, drops the last entry
  (it may be incomplete) and appends the rest.
- **Extractions:** `-c` skips files whose size matches their archived size.
  Anything cut short is written again.

Without `-c`, an existing archive is overwritten.

Extraction refuses entries with absolute paths or `..`, so an archive can't
write outside `OUTPUT_DIR`.

### Archive format

A `.tard` file is a plain sequence of entries, with no header or index:

```
u64 LE path length | path bytes | u64 LE content length | content bytes
```

Paths are relative to the archived directory's parent. They're stored as
raw bytes on Unix and as UTF-8 with `/` separators on Windows, so archives
move between the two. If a file shrinks or fails while it's being read, it
is stored at the length actually read. If its header was already written
by then, the missing bytes are zero-filled instead, as tar does. Either way
the archive stays parseable and a warning is printed.

## The server

`tard server` runs jobs in the background. It listens on `127.0.0.1` only,
and clients authenticate with a token in `~/.tard/server/token` (readable by
your user only).

```sh
tard server ~/photos /backup        # submit a job (starts the server if needed)
tard server ls                      # what's running, queued, paused
tard server log 12                  # follow job 12's log (or: log photos)
tard server stop                    # stop; running jobs resume on next start
```

### Submitting jobs

A job takes the same arguments as `tard` itself. Jobs can be chained:

| Form | Runs |
|---|---|
| `A` | `A` after everything already queued |
| `A -q B` | `A`, then `B` |
| `A -p B` | `A` and `B` side by side |
| `A -p B -q C` | `A` and `B` together, then `C` (`-p` binds tighter than `-q`) |
| `-p A` | `A` right away, alongside whatever is running, skipping the queue |

Each `A`, `B`, `C` stands for a full set of job arguments, e.g.
`~/photos /backup -r 64MB`. Submissions run one at a time in the order they
arrive. If a job fails, the rest of its chain is skipped. Relative paths are
resolved where you run the command.

### Resources

The server shares `max_ram` and `max_workers` across at most
`max_parallel_jobs` jobs:

- **With `-r`/`-n`:** the job gets what it asked for, capped at the server's
  total. It waits in the queue until there's room.
- **Without them:** the server picks, giving the job a share of what's free.
  That's split across the open job slots and kept within `[jobs]` `min_*`
  and `max_*`.
- **Order is strict:** a big job at the front isn't overtaken by smaller ones
  behind it.

### Managing jobs

| Command | Does |
|---|---|
| `ls` | List running, queued and paused jobs with their resources |
| `log <ID\|NAME>` | Print a job's log by id, or by input name (latest match), following it while it runs. Also works with the server stopped |
| `drop <ID>...` | Stop a running job, or remove a waiting one |
| `pause <ID>...` | Stop a running job, or hold a queued one |
| `resume <ID>...` | Continue paused jobs (with `-c`), ahead of the queue |
| `edit <ID> [IN] [OUT] [OPTIONS]` | Change a queued or paused job. Options given replace the job's; `-r auto` and `-n auto` let the server decide; `-l default` restores the default log location |
| `watch` | Take over the terminal with a live view of jobs and server output; `q` gives it back |

### Starting and stopping

| Command | Does |
|---|---|
| `start` (or bare `tard server`) | Start in the background |
| `stop` | Stop. Running jobs are dropped and saved, with the queue, then resumed (with `-c`) on the next start |
| `stop --wait` | Let running jobs finish first |
| `run` | Run in the foreground, e.g. under systemd |
| `ping` | Show whether it's running |

A job running longer than `max_run_time` is dropped.

### Scheduling

```sh
tard server schedule -n nightly     # create one, step by step
tard server schedule                # list scheduled jobs
tard server schedule nightly        # details and recent runs
```

`-n` asks for the job's arguments (checked like a submission), how many runs
(`once`, a number, or `series` to repeat forever), how often, and when to
start.

- **How often:** `every 30m`, `every 6h`, `every 1d` or `every 2w`, or days
  and a time: `mon,thu 03:00`, `daily 03:00`, `weekdays 09:30`,
  `weekends 10:00`. Times are local.
- **Starting:** scheduled runs start on time alongside the queue, waiting
  only for RAM and workers.
- **Overlap:** if a run's previous run is still going, that run is skipped.
- **Downtime:** runs missed while the server was down are made up once when
  it starts.

| Option | Does |
|---|---|
| `-n`, `--new NAME` | Create a scheduled job |
| `-e`, `--edit NAME\|ID` | Edit a scheduled job (same prompts, current values as defaults), or a queued job by id |
| `-r`, `--remove NAME\|ID...` | Remove scheduled jobs, or drop jobs by id |
| `-l`, `--log NAME\|ID` | The latest run's log, or a job's log by id |

## Logs

| What | Where |
|---|---|
| Per-file CSV log of each run (name, directory, size, mtime) | `~/.tard/logs/<name>_<datetime>.log`, or `-l <DIR>` / `-l <FILE>` |
| Server job logs (command, resources, output, outcome) | `~/.tard/server/logs/<datetime>_<id>_<input>.log` |
| Scheduled runs' job logs | `~/.tard/server/logs/scheduled/<name>/` |
| Server events | `~/.tard/server/server.log` |

`[logs] keep` (default 100) caps each log directory; the oldest go first.
Directories given with `-l` are never pruned.

## Configuration

`~/.tard/config.toml` is written with commented defaults the first time tard
runs. Set `TARD_HOME` to use another directory instead of `~/.tard`.

| Key | Default | Meaning |
|---|---|---|
| `server.port` | `8273` | Localhost port |
| `server.max_ram` | `"auto"` | RAM shared by all jobs; `auto` = half of the memory available at server start |
| `server.max_workers` | `"auto"` | Worker threads shared by all jobs; `auto` = number of CPUs |
| `server.max_parallel_jobs` | `4` | Jobs running at once |
| `server.max_run_time` | `"none"` | Drop jobs running longer than this, e.g. `"12h"` |
| `jobs.min_ram` / `jobs.max_ram` | `"10MB"` / `"256MB"` | Bounds for jobs the server sizes |
| `jobs.min_workers` / `jobs.max_workers` | `1` / `8` | Bounds for jobs the server sizes |
| `jobs.express_when_full` | `"wait"` | A leading `-p` job with no room: `wait`, or `shrink` to start with what's free |
| `logs.keep` | `100` | Logs kept per log directory |
| `logs.files` | `~/.tard/logs` | Directory for per-file CSV logs |
| `schedule.history` | `10` | Past runs kept per scheduled job |

Sizes are written like `512KB`, `64MB` or `4GB`; durations like `30m`,
`12h`, `2d` or `1w`.
