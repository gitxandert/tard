// `tard server ...`: everything except `run` is sent to the daemon.
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    net::TcpStream,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use crate::config::Config;
use crate::server::proto::{self, Frame};
use crate::server::{create_private_dir, daemon, jobs, prompt, server_dir, server_log_path, token_path};
use crate::utils::path::path_to_bytes;

const START_TIMEOUT: Duration = Duration::from_secs(5);

/// Entry for `tard server <args>`; returns the process exit code.
pub fn main(args: Vec<OsString>) -> i32 {
    let config = match Config::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error in config: {}", e);
            return 2;
        }
    };
    match args.first().and_then(|a| a.to_str()) {
        None | Some("start") => start(&config),
        Some("run") => match daemon::run() {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("Error: {}", e);
                1
            }
        },
        Some("-h") | Some("--help") => {
            print_help();
            0
        }
        Some("stop") => send_or(&config, &args, "server is not running"),
        Some("log") => match send(&config, &args) {
            Ok(code) => code,
            Err(Unreachable) => log_offline(&args[1..]),
        },
        Some("ping") | Some("ls") | Some("drop") | Some("pause") | Some("resume") => {
            send_or(&config, &args, "server is not running (start it with: tard server start)")
        }
        Some("edit") => {
            let mut req = args[..args.len().min(2)].to_vec();
            match jobs::absolutize(args.into_iter().skip(2).collect()) {
                Ok(t) => req.extend(t),
                Err(e) => {
                    eprintln!("Error: {}", e);
                    return 2;
                }
            }
            send_or(&config, &req, "server is not running (start it with: tard server start)")
        }
        Some("schedule") => schedule(&config, &args[1..]),
        Some("watch") => {
            use std::io::IsTerminal;
            if !io::stdout().is_terminal() {
                eprintln!("watch needs a terminal; for a plain stream use: tail -f {}", server_log_path().display());
                return 2;
            }
            let fetch = |a: &[&str]| -> Option<String> {
                let args: Vec<OsString> = a.iter().map(OsString::from).collect();
                let mut out = Vec::new();
                request(&config, &args, &mut out, &mut io::sink()).ok()?;
                Some(String::from_utf8_lossy(&out).into_owned())
            };
            match crate::server::watch::run(&config, &fetch) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    1
                }
            }
        }
        // anything else is a job: tard server <input> <output> [options]
        Some(_) => submit(&config, args),
    }
}

/// The server didn't answer on its port.
struct Unreachable;

fn send_or(config: &Config, args: &[OsString], unreachable: &str) -> i32 {
    send(config, args).unwrap_or_else(|Unreachable| {
        eprintln!("{}", unreachable);
        1
    })
}

fn is_up(config: &Config) -> bool {
    matches!(request(config, &[OsString::from("ping")], &mut io::sink(), &mut io::sink()), Ok(0))
}

fn start(config: &Config) -> i32 {
    if !is_up(config) {
        if let Err(code) = launch(config) {
            return code;
        }
    }
    send_or(config, &[OsString::from("ping")], "server stopped right after starting")
}

/// Checks the jobs here, where relative paths mean something, then hands them to the
/// server, starting the server first if needed.
fn submit(config: &Config, args: Vec<OsString>) -> i32 {
    let parsed = jobs::split_chain(args).and_then(|(lane, groups)| {
        let groups = groups.into_iter()
            .map(|g| g.into_iter().map(jobs::parse_job).collect::<Result<Vec<_>, _>>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((lane, groups))
    });
    let (lane, groups) = match parsed {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: {}", e);
            return 2;
        }
    };
    if !is_up(config) {
        if let Err(code) = launch(config) {
            return code;
        }
    }
    let mut req = vec![OsString::from("submit")];
    req.extend(jobs::join_chain(lane, &groups));
    send_or(config, &req, "server stopped before the job was sent")
}

/// `tard server schedule ...`. The flags mirror the top-level commands and also take
/// job ids, so `schedule -r 12` is `drop 12` and `schedule -l 12` is `log 12`.
fn schedule(config: &Config, args: &[OsString]) -> i32 {
    // the top-level job verbs work here too, with flag spellings as aliases
    let verb = match args.first().and_then(|a| a.to_str()) {
        None | Some("ls") => "ls",
        Some("-h" | "--help") => {
            print_schedule_help();
            return 0;
        }
        Some("new" | "-n" | "--new") => "new",
        Some("edit" | "-e" | "--edit") => "edit",
        Some("drop" | "remove" | "-r" | "--remove") => "drop",
        Some("log" | "-l" | "--log") => "log",
        Some("pause") => "pause",
        Some("resume") => "resume",
        Some(f) if f.starts_with('-') => {
            eprintln!("unknown option {} (see tard server schedule -h)", f);
            return 2;
        }
        Some(_) => "show",
    };
    let targets: Vec<String> = match verb {
        "ls" => Vec::new(),
        "show" => vec![args[0].to_string_lossy().into_owned()],
        _ => args[1..].iter().map(|a| a.to_string_lossy().into_owned()).collect(),
    };
    if verb != "ls" && targets.is_empty() {
        eprintln!("{} needs a scheduled job name (or a job id); see tard server schedule -h", verb);
        return 2;
    }
    if !is_up(config) {
        if let Err(code) = launch(config) {
            return code;
        }
    }
    let is_id = |t: &str| t.chars().all(|c| c.is_ascii_digit());
    let os = |v: &[&str]| v.iter().map(OsString::from).collect::<Vec<_>>();
    let down = "server stopped unexpectedly";
    let first = targets.first().map(String::as_str).unwrap_or("");
    match verb {
        "ls" => send_or(config, &os(&["schedule"]), down),
        "show" => send_or(config, &os(&["schedule", first]), down),
        "new" => {
            if let Err(e) = crate::server::schedule::check_name(first) {
                eprintln!("Error: {}", e);
                return 2;
            }
            if matches!(request(config, &os(&["schedule-get", first]), &mut io::sink(), &mut io::sink()), Ok(0)) {
                eprintln!("a scheduled job named '{}' already exists (edit it with: tard server schedule edit {})", first, first);
                return 1;
            }
            run_prompts(config, first, None)
        }
        "edit" if is_id(first) => {
            let mut req = vec![OsString::from("edit"), first.into()];
            match jobs::absolutize(args[2..].to_vec()) {
                Ok(rest) => req.extend(rest),
                Err(e) => { eprintln!("Error: {}", e); return 2; }
            }
            send_or(config, &req, down)
        }
        "edit" => {
            let mut out = Vec::new();
            match request(config, &os(&["schedule-get", first]), &mut out, &mut io::stderr()) {
                Ok(0) => (),
                Ok(code) => return code,
                Err(Unreachable) => { eprintln!("{}", down); return 1; }
            }
            let Some(cur) = prompt::Current::parse(&String::from_utf8_lossy(&out)) else {
                eprintln!("Error: unexpected reply from server");
                return 1;
            };
            run_prompts(config, first, Some(&cur))
        }
        "log" => {
            let v = if is_id(first) { "log" } else { "schedule-log" };
            send_or(config, &os(&[v, first]), down)
        }
        // drop / pause / resume: ids go to the job commands, names to the schedule
        _ => {
            let mut code = 0;
            for t in &targets {
                let req = if is_id(t) {
                    os(&[verb, t])
                } else {
                    os(&[&format!("schedule-{}", verb), t])
                };
                code = code.max(send_or(config, &req, down));
            }
            code
        }
    }
}

fn run_prompts(config: &Config, name: &str, current: Option<&prompt::Current>) -> i32 {
    match prompt::schedule_request(name, current) {
        Ok(Some(req)) => send_or(config, &req, "server stopped before the schedule was saved"),
        Ok(None) => {
            println!("not saved");
            1
        }
        Err(e) => {
            eprintln!("{}", e);
            1
        }
    }
}

fn print_schedule_help() {
    println!(
"Usage: tard server schedule [COMMAND] [NAME | JOB ID]...

Run jobs at set times. Scheduled runs start at their time, alongside the queue
(waiting for room if needed), and log to {}/<NAME>/.
The job commands work here too: given a job id instead of a name, edit, drop,
log, pause and resume act on that job, as `tard server <command> <id>` would.

  ls                   List scheduled jobs (also: bare `schedule`)
  NAME                 Show a scheduled job: its arguments, how often it runs,
                       its next run, and its last runs with their status
  new NAME             Create a scheduled job, step by step: the job's
                       arguments, then how many runs (once, N, or series),
                       then when it first runs
  edit NAME|ID         Change a scheduled job (same steps, current values as
                       defaults)
  drop NAME|ID...      Remove scheduled jobs (runs already going finish)
  log NAME|ID          Print the latest run's log, following it while it runs
  pause NAME|ID...     Stop a scheduled job from running until it's resumed
  resume NAME|ID...    Let it run again; a run that came due while paused
                       runs once right away

Flag spellings also work: -n/--new, -e/--edit, -r/--remove, -l/--log.

Repeats: every 30m / 6h / 1d / 2w, or days and a time: mon,thu 03:00,
daily 03:00, weekdays 09:30, weekends 10:00. Times are local. Runs missed while
the server was down are run once when it starts.",
        jobs::logs_dir().join("scheduled").display(),
    );
}

/// Starts the server and waits until it answers.
fn launch(config: &Config) -> Result<(), i32> {
    if let Err(e) = spawn_daemon() {
        eprintln!("Error: could not start server: {}", e);
        return Err(1);
    }
    let deadline = Instant::now() + START_TIMEOUT;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        // a stale token can be read before the new server writes its own; retry until it answers
        if is_up(config) {
            return Ok(());
        }
    }
    eprintln!("Error: server did not come up within {}s. Last lines of {}:", START_TIMEOUT.as_secs(), server_log_path().display());
    print_tail(&server_log_path(), 5);
    Err(1)
}

/// `log` with the server down: print the latest matching log as it stands.
fn log_offline(args: &[OsString]) -> i32 {
    let [key] = args else {
        eprintln!("usage: tard server log <job id | input name>");
        return 2;
    };
    let key = key.to_string_lossy();
    let logs = jobs::find_logs(&key);
    let Some(path) = logs.last() else {
        eprintln!("no log found for '{}'", key);
        return 1;
    };
    match fs::read(path) {
        Ok(b) => {
            let _ = io::stdout().write_all(&b);
            0
        }
        Err(e) => {
            eprintln!("Error: could not read {}: {}", path.display(), e);
            1
        }
    }
}

/// Starts `tard server run` detached, its output appended to server.log.
fn spawn_daemon() -> io::Result<()> {
    create_private_dir(&server_dir())?;
    let mut o = OpenOptions::new();
    o.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    let log = o.open(server_log_path())?;
    let mut cmd = Command::new(std::env::current_exe()?);
    cmd.args(["server", "run"])
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0); // out of the terminal's job control, so closing it won't stop the server
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // Not DETACHED_PROCESS: a server with no console at all makes Windows open a new
        // console window for every job it starts. CREATE_NO_WINDOW gives it a hidden
        // console instead, which its jobs share.
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x200;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
    }
    // don't pin the caller's working directory (it could be unmounted later)
    cmd.current_dir(server_dir()).spawn().map(drop)
}

/// Sends `args` and copies the reply to stdout/stderr.
fn send(config: &Config, args: &[OsString]) -> Result<i32, Unreachable> {
    let mut out = io::stdout();
    let mut err = io::stderr();
    request(config, args, &mut out, &mut err)
}

fn request<O: Write, E: Write>(config: &Config, args: &[OsString], out: &mut O, err: &mut E) -> Result<i32, Unreachable> {
    let stream = TcpStream::connect(("127.0.0.1", config.server.port)).map_err(|_| Unreachable)?;
    let token = fs::read(token_path()).unwrap_or_default();
    let args: Vec<Vec<u8>> = match args.iter().map(|a| path_to_bytes(Path::new(a))).collect() {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(err, "Error: {}", e);
            return Ok(2);
        }
    };
    if proto::write_request(&mut BufWriter::new(&stream), &token, &args).is_err() {
        return Err(Unreachable);
    }
    let mut r = BufReader::new(&stream);
    loop {
        match proto::read_frame(&mut r) {
            Ok(Frame::Out(b)) => { let _ = out.write_all(&b); let _ = out.flush(); }
            Ok(Frame::Err(b)) => { let _ = err.write_all(&b); let _ = err.flush(); }
            Ok(Frame::Exit(code)) => return Ok(code),
            Err(e) => {
                let _ = writeln!(err, "Error: lost connection to server: {}", e);
                return Ok(1);
            }
        }
    }
}

fn print_tail(path: &Path, n: usize) {
    if let Ok(text) = fs::read_to_string(path) {
        let lines: Vec<&str> = text.lines().collect();
        for l in &lines[lines.len().saturating_sub(n)..] {
            eprintln!("  {}", l);
        }
    }
}

fn print_help() {
    println!(
"Usage: tard server [COMMAND]
       tard server <INPUT> <OUTPUT_DIR> [OPTIONS]

Run tard jobs from a background server. Settings live in {}.

Jobs:
  <INPUT> <OUTPUT_DIR> [OPTIONS]
             Queue a job, starting the server if needed. Takes the same options
             as tard itself (-a, -x, -c, -v, -r, -n, -l); see tard -h. Without -r/-n
             the server sizes the job from what's free
  JOB -q JOB Run the second job after the first (and so on). If a job fails,
             the rest of its chain is skipped
  JOB -p JOB Run both jobs side by side; -p binds tighter than -q, so
             `A -p B -q C` runs A and B together, then C
  -p JOB ... Run now, alongside whatever is running, instead of joining the
             queue (waits for room unless [jobs] express_when_full = \"shrink\")
  ls         List running, queued and paused jobs
  log <ID|NAME>
             Print a job's log, by job id or input file name (latest match),
             following it while the job runs
  drop <ID>...
             Stop a running job or remove a waiting one. A dropped archive can
             be resumed with -c
  pause <ID>...
             Stop a running job (or hold a queued one) until it's resumed
  resume <ID>...
             Continue paused jobs (with -c) as soon as there's room
  edit <ID> [<INPUT>] [<OUTPUT_DIR>] [OPTIONS]
             Change a queued or paused job: options given replace the job's,
             -r auto / -n auto let the server decide, -l default restores the
             default CSV log location, paths replace its input (then output)
  schedule [...]
             Run jobs at set times; see tard server schedule -h
  watch      Take over the terminal with a live view of jobs and server
             output; q gives it back

Server:
  start      Start the server in the background (also: bare `tard server`)
  stop [--wait]
             Stop the server. Running jobs are dropped and saved, with the
             queue, to resume on the next start; --wait lets them finish first
  run        Run the server in the foreground (for systemd and debugging)
  ping       Show whether the server is running
  -h, --help Show this help

Job logs are in {}; server output is appended to {}.",
        crate::config::config_path().display(),
        jobs::logs_dir().display(),
        server_log_path().display(),
    );
}
