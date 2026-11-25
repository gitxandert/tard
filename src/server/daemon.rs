// The long-running server: `tard server run` (start spawns it detached).
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread,
    time::{Duration, SystemTime},
};
use crate::config::{Config, WhenFull};
use crate::server::jobs::{self, Job, JobId};
use crate::server::proto::{self, Frame};
use crate::server::queue::{Chain, Lane, Limits, Queue, Res, Running, Spot, StopRequest};
use crate::server::schedule::{self, Run, Schedule, Zdt};
use crate::server::{create_private_dir, create_private_file, pid_path, server_dir, token_path};
use crate::utils::formatting::{format_duration, format_size};
use crate::utils::logger::format_iso_time;
use crate::utils::path::bytes_to_path;

const POLL: Duration = Duration::from_millis(200);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

struct Daemon {
    token: Vec<u8>,
    config: Config,
    limits: Limits,
    started: SystemTime,
    exe: PathBuf, // this binary, run for each job
    state: Mutex<State>,
    /// signalled whenever a running job finishes
    finished: Condvar,
    /// signalled when schedules change, so the scheduler rechecks its next wake-up
    sched_wake: Condvar,
}

struct State {
    next_id: JobId,
    q: Queue,
    schedules: Vec<Schedule>,
    stopping: bool,
}

/// Runs the server in the foreground until `tard server stop`.
pub fn run() -> Result<(), String> {
    let config = Config::load()?;
    let dir = server_dir();
    create_private_dir(&dir).map_err(|e| format!("could not create {}: {}", dir.display(), e))?;
    create_private_dir(&jobs::logs_dir()).map_err(|e| format!("could not create log dir: {}", e))?;
    let exe = std::env::current_exe().map_err(|e| format!("could not find the tard binary: {}", e))?;

    // bind before touching the token, so a second `start` can't clobber a live server's token
    let port = config.server.port;
    let listener = TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("could not listen on 127.0.0.1:{}: {}", port, e))?;

    let token = new_token();
    write_private(&token_path(), &token)?;
    write_private(&pid_path(), std::process::id().to_string().as_bytes())?;

    let limits = Limits {
        total: Res { ram: config.server_ram(), workers: config.server_workers() },
        max_jobs: config.server.max_parallel_jobs,
        min: Res { ram: config.jobs.min_ram, workers: config.jobs.min_workers },
        max: Res { ram: config.jobs.max_ram, workers: config.jobs.max_workers },
        shrink_express: config.jobs.express_when_full == WhenFull::Shrink,
    };
    event(&format!(
        "server started on 127.0.0.1:{} (pid {}); sharing {} and {} workers across up to {} jobs",
        port, std::process::id(), fmt_ram(limits.total.ram), limits.total.workers, limits.max_jobs,
    ));

    let (saved, bad) = jobs::load_queue();
    for (id, why) in bad {
        event(&format!("job {} dropped from the saved queue: {}", id, why));
    }
    if !saved.is_empty() {
        event(&format!("restored {} waiting job(s)", saved.len()));
    }
    let schedules = schedule::load()?;
    let next_id = read_next_id()
        .max(jobs::max_logged_id() + 1)
        .max(saved.iter().map(|(j, ..)| j.id + 1).max().unwrap_or(1));

    let daemon = Arc::new(Daemon {
        token,
        config,
        limits,
        started: SystemTime::now(),
        exe,
        state: Mutex::new(State { next_id, q: Queue::restore(saved), schedules, stopping: false }),
        finished: Condvar::new(),
        sched_wake: Condvar::new(),
    });
    save(&daemon.lock().q); // drops any jobs that failed to load
    kick(&daemon);
    let d = Arc::clone(&daemon);
    thread::spawn(move || scheduler(d)); // runs anything missed while the server was down

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let d = Arc::clone(&daemon);
                thread::spawn(move || {
                    if let Err(e) = handle(&d, s) {
                        // a client hanging up mid-reply (e.g. Ctrl-C on `log`) is normal
                        if !matches!(e.kind(), io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset) {
                            event(&format!("connection error: {}", e));
                        }
                    }
                });
            }
            Err(e) => event(&format!("accept failed: {}", e)),
        }
    }
    Ok(())
}

/// One line of server output, stamped with local time (stdout, which `start` points at
/// server.log).
pub fn event(msg: &str) {
    println!("{} {}", Zdt::now().strftime("%Y-%m-%d %H:%M:%S"), msg);
}

impl Daemon {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

fn save(q: &Queue) {
    if let Err(e) = jobs::save_queue(&q.saved()) {
        event(&format!("could not save the queue: {}", e));
    }
}

fn fmt_ram(n: usize) -> String {
    format_size(n as u64).replace(' ', "")
}

// running jobs

/// Starts whatever the queue says can run now. Called after anything that adds work or
/// frees resources. Returns the ids started.
fn kick(d: &Arc<Daemon>) -> Vec<JobId> {
    let starts: Vec<(Job, Res, PathBuf)> = {
        let mut st = d.lock();
        if st.stopping {
            return Vec::new();
        }
        let planned = st.q.plan(&d.limits);
        let now = SystemTime::now();
        let starts = planned.into_iter().map(|s| {
            let dir = jobs::log_dir_for(&s.job);
            let _ = create_private_dir(&dir);
            let log = jobs::new_log_path(&dir, s.job.id, &s.job.args, now);
            // into running under the same lock, so `ls` and `drop` always see the job
            st.q.running.push(Running { job: s.job.clone(), chain: s.chain, alloc: s.alloc, log: log.clone(), started: now, stop: None });
            (s.job, s.alloc, log)
        }).collect();
        if !st.q.running.is_empty() {
            save(&st.q);
        }
        starts
    };
    let mut ids = Vec::new();
    for (job, alloc, log) in starts {
        ids.push(job.id);
        let started = SystemTime::now();
        match spawn_job(d, &job, alloc, &log) {
            Ok(child) => {
                event(&format!("job {} started with {}, {} workers: {}", job.id, fmt_ram(alloc.ram), alloc.workers, jobs::describe(&job.args)));
                let d = Arc::clone(d);
                thread::spawn(move || supervise(d, job.id, child, log, started));
            }
            Err(e) => {
                append(&log, &format!("--- could not start: {}\n", e));
                event(&format!("job {} could not start: {}", job.id, e));
                let d = Arc::clone(d);
                // finish() kicks again, so don't do it while this kick is still unwinding
                thread::spawn(move || finish(&d, job.id, Ended::Failed, "could not start".into(), Duration::ZERO));
            }
        }
    }
    ids
}

fn spawn_job(d: &Daemon, job: &Job, alloc: Res, log: &Path) -> io::Result<Child> {
    let mut args = job.args.clone();
    args.max_ram = alloc.ram;
    args.num_workers = alloc.workers;
    args.ram_given = true;
    args.workers_given = true;
    let argv = args.to_argv();
    let mut f = create_private_file(log)?;
    writeln!(f, "job {}: {}", job.id, jobs::describe(&job.args))?;
    writeln!(f, "command: tard {}", argv.iter().map(|a| a.to_string_lossy()).collect::<Vec<_>>().join(" "))?;
    writeln!(f, "resources: {}, {} workers", fmt_ram(alloc.ram), alloc.workers)?;
    writeln!(f, "submitted {}", format_iso_time(job.submitted))?;
    writeln!(f, "started   {}", format_iso_time(SystemTime::now()))?;
    writeln!(f, "---")?;
    let mut cmd = Command::new(&d.exe);
    cmd.args(&argv)
        .stdin(Stdio::null())
        .stdout(f.try_clone()?)
        .stderr(f)
        .current_dir(server_dir());
    #[cfg(windows)]
    {
        // no console window per job, even when the server was started without one
        // (e.g. `tard server run` from a service)
        use std::os::windows::process::CommandExt;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(CREATE_NO_WINDOW);
    }
    cmd.spawn()
}

enum Outcome {
    Exited(ExitStatus),
    Stopped(StopRequest),
    TimedOut(Duration),
    Lost(io::Error),
}

/// How a run ended, as far as the queue cares.
enum Ended {
    Ok,
    Failed,
    Stopped(StopRequest),
}

/// Waits for one job, killing it if it's dropped, paused, or runs past max_run_time.
fn supervise(d: Arc<Daemon>, id: JobId, mut child: Child, log: PathBuf, started: SystemTime) {
    let max = d.config.server.max_run_time;
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Outcome::Exited(status),
            Err(e) => break Outcome::Lost(e),
            Ok(None) => (),
        }
        let stop = d.lock().q.running.iter().find(|r| r.job.id == id).and_then(|r| r.stop);
        if let Some(req) = stop {
            let _ = child.kill();
            let _ = child.wait();
            break Outcome::Stopped(req);
        }
        if let Some(m) = max.filter(|m| started.elapsed().unwrap_or_default() > *m) {
            let _ = child.kill();
            let _ = child.wait();
            break Outcome::TimedOut(m);
        }
        thread::sleep(POLL);
    };

    let ran = started.elapsed().unwrap_or_default();
    let took = format_duration(ran);
    let (summary, ended) = match outcome {
        Outcome::Exited(s) if s.success() => (format!("finished in {}", took), Ended::Ok),
        Outcome::Exited(s) => (match s.code() {
            Some(c) => format!("failed (exit {}) after {}", c, took),
            None => format!("failed (killed by a signal) after {}", took),
        }, Ended::Failed),
        Outcome::Stopped(StopRequest::Drop) => (format!("dropped after {}", took), Ended::Stopped(StopRequest::Drop)),
        Outcome::Stopped(StopRequest::Pause) => (format!("paused after {} (resume continues it)", took), Ended::Stopped(StopRequest::Pause)),
        Outcome::Stopped(StopRequest::Requeue) => (format!("stopped with the server after {}; resumes on next start", took), Ended::Stopped(StopRequest::Requeue)),
        Outcome::TimedOut(m) => (format!("dropped: ran past max_run_time ({})", format_duration(m)), Ended::Failed),
        Outcome::Lost(e) => (format!("lost track of the process: {}", e), Ended::Failed),
    };
    append(&log, &format!("--- {} ({})\n", summary, format_iso_time(SystemTime::now())));
    event(&format!("job {} {}", id, summary));
    // the summary up to " after"/" in" is the short status kept in schedule history
    let status = summary.split(" after ").next().unwrap_or(&summary).split(" in ").next().unwrap_or(&summary);
    let status = if status == "finished" { "ok" } else { status };
    finish(&d, id, ended, status.to_string(), ran);
}

/// Takes a job out of `running` once its log is complete, then starts what can run.
fn finish(d: &Arc<Daemon>, id: JobId, ended: Ended, status: String, ran: Duration) {
    let mut st = d.lock();
    if let Ended::Stopped(StopRequest::Requeue) = ended {
        if let Some(r) = st.q.running.iter().find(|r| r.job.id == id) {
            let (job, chain) = (r.job.clone(), r.chain);
            st.q.put_back(job, chain);
        }
    }
    let (r, skipped) = st.q.finished(id, matches!(ended, Ended::Failed));
    if let (Some(r), Ended::Stopped(StopRequest::Pause)) = (&r, &ended) {
        let mut job = r.job.clone();
        job.args.resume = true;
        st.q.paused.push(job);
    }
    for j in &skipped {
        event(&format!("job {} skipped: job {} earlier in its chain failed", j.id, id));
    }
    save(&st.q);
    if let Some(name) = r.as_ref().and_then(|r| r.job.schedule.clone()) {
        let entry = st.schedules.iter_mut().find(|s| s.name == name)
            .and_then(|s| s.history.iter_mut().rev().find(|h| h.job == id));
        if let Some(h) = entry {
            h.status = status;
            h.secs = ran.as_secs();
            save_schedules(&st.schedules);
        }
    }
    let live: Vec<&Path> = st.q.running.iter().map(|r| r.log.as_path()).collect();
    if let Some(dir) = r.as_ref().and_then(|r| r.log.parent()) {
        jobs::prune_logs(dir, d.config.logs.keep, &live);
    }
    drop(st);
    d.finished.notify_all();
    kick(d);
}

fn append(log: &Path, text: &str) {
    if let Ok(mut f) = OpenOptions::new().append(true).open(log) {
        let _ = f.write_all(text.as_bytes());
    }
}

// requests

fn handle(d: &Arc<Daemon>, stream: TcpStream) -> io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let (token, args) = proto::read_request(&mut BufReader::new(&stream))?;
    let mut reply = Reply(BufWriter::new(&stream));
    if !constant_time_eq(&token, &d.token) {
        reply.err("server rejected the request: bad token (is another server on this port?)")?;
        return reply.exit(1);
    }
    let args: Vec<OsString> = args.into_iter()
        .map(|a| bytes_to_path(a).map(|p| p.into_os_string()))
        .collect::<io::Result<_>>()?;
    let Some(cmd) = args.first().and_then(|a| a.to_str()) else {
        reply.err("empty request")?;
        return reply.exit(2);
    };
    let rest = &args[1..];
    let ids_usage = |verb: &str| format!("usage: tard server {} <job id>...", verb);

    let code = match cmd {
        "ping" => {
            let up = d.started.elapsed().unwrap_or_default();
            reply.out(&format!(
                "server running on 127.0.0.1:{} (pid {}), up {}",
                d.config.server.port, std::process::id(), format_duration(up),
            ))?;
            0
        }
        "submit" => submit(d, &mut reply, rest.to_vec())?,
        "ls" => ls(d, &mut reply)?,
        "log" => match rest {
            [key] => log(d, &mut reply, &key.to_string_lossy())?,
            _ => { reply.err("usage: tard server log <job id | input name>")?; 2 }
        },
        "drop" | "pause" | "resume" if rest.is_empty() => { reply.err(&ids_usage(cmd))?; 2 }
        "drop" | "pause" | "resume" => for_each_id(d, &mut reply, cmd, rest)?,
        "edit" => edit(d, &mut reply, rest)?,
        "schedule" => match rest {
            [] => schedule_list(d, &mut reply)?,
            [name] => schedule_show(d, &mut reply, &name.to_string_lossy())?,
            _ => { reply.err("usage: tard server schedule [<name>]")?; 2 }
        },
        "schedule-add" | "schedule-set" => schedule_put(d, &mut reply, cmd == "schedule-set", rest)?,
        "schedule-get" | "schedule-rm" | "schedule-drop" | "schedule-log" | "schedule-pause" | "schedule-resume" => match rest {
            [name] => {
                let name = name.to_string_lossy();
                match cmd {
                    "schedule-get" => schedule_get(d, &mut reply, &name)?,
                    "schedule-rm" | "schedule-drop" => schedule_rm(d, &mut reply, &name)?,
                    "schedule-log" => schedule_log(d, &mut reply, &name)?,
                    _ => schedule_pause(d, &mut reply, &name, cmd == "schedule-pause")?,
                }
            }
            _ => { reply.err(&format!("usage: {} <name>", cmd))?; 2 }
        },
        "stop" => {
            let wait = rest.iter().any(|a| a == "--wait" || a == "-w");
            stop(d, &mut reply, wait)?;
            reply.exit(0)?;
            shutdown()
        }
        other => {
            reply.err(&format!("unknown server command '{}' (see tard server -h)", other))?;
            2
        }
    };
    reply.exit(code)
}

fn submit(d: &Arc<Daemon>, reply: &mut Reply, tokens: Vec<OsString>) -> io::Result<i32> {
    let parsed = jobs::split_chain(tokens).and_then(|(lane, groups)| {
        let groups = groups.into_iter()
            .map(|g| g.into_iter().map(jobs::parse_job).collect::<Result<Vec<_>, _>>())
            .collect::<Result<Vec<_>, _>>()?;
        Ok((lane, groups))
    });
    let (lane, groups) = match parsed {
        Ok(p) => p,
        Err(e) => {
            reply.err(&format!("Error: {}", e))?;
            return Ok(2);
        }
    };

    let mut st = d.lock();
    if st.stopping {
        reply.err("server is stopping; job not accepted")?;
        return Ok(1);
    }
    let now = SystemTime::now();
    let mut ids: Vec<Vec<JobId>> = Vec::new();
    let groups = groups.into_iter().map(|g| {
        let mut gi = Vec::new();
        let g = g.into_iter().map(|args| {
            let id = st.next_id;
            st.next_id += 1;
            gi.push(id);
            Job { id, args, submitted: now, schedule: None }
        }).collect::<Vec<_>>();
        ids.push(gi);
        g
    }).collect();
    write_next_id(st.next_id);
    let chain_id = ids[0][0];
    st.q.submit(Chain { id: chain_id, lane, groups });
    let descs: Vec<(JobId, String)> = st.q.waiting().iter()
        .filter(|(j, _)| ids.iter().flatten().any(|i| *i == j.id))
        .map(|(j, _)| (j.id, jobs::describe(&j.args)))
        .collect();
    save(&st.q);
    drop(st);
    for (id, desc) in &descs {
        event(&format!("job {} submitted: {}", id, desc));
    }

    let started = kick(d);
    let st = d.lock();
    let spots = st.q.waiting();
    for (gi, g) in ids.iter().enumerate() {
        for (ji, id) in g.iter().enumerate() {
            let desc = &descs.iter().find(|(i, _)| i == id).unwrap().1;
            let when = if started.contains(id) {
                "started".to_string()
            } else {
                let after = match (ji, gi) {
                    (0, 0) => String::new(),
                    (0, _) => format!(", after {}", join_ids(&ids[gi - 1])),
                    _ => format!(", alongside {}", g[0]),
                };
                match spots.iter().find(|(j, _)| j.id == *id).map(|(_, s)| s) {
                    Some(Spot::Queued(n)) => format!("queued (#{} in line{})", n, after),
                    Some(Spot::Express) => format!("waiting for room ahead of the queue{}", after),
                    // finished or failed to start in the meantime
                    _ => "submitted".to_string(),
                }
            };
            reply.out(&format!("job {} {}: {}", id, when, desc))?;
        }
    }
    drop(st);
    reply.out(&format!("  follow with: tard server log <id>   (e.g. tard server log {})", chain_id))?;
    Ok(0)
}

fn join_ids(ids: &[JobId]) -> String {
    ids.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
}

fn ls(d: &Daemon, reply: &mut Reply) -> io::Result<i32> {
    let st = d.lock();
    let waiting = st.q.waiting();
    if st.q.running.is_empty() && waiting.is_empty() {
        reply.out("no jobs running, queued or paused")?;
        return Ok(0);
    }
    let row = |id: &dyn std::fmt::Display, state: &str, time: &str, res: &str, job: &str| {
        format!("{:>4}  {:<9} {:>7}  {:<14}  {}", id, state, time, res, job)
    };
    reply.out(&row(&"ID", "STATE", "TIME", "RESOURCES", "JOB"))?;
    for r in &st.q.running {
        let state = match r.stop {
            None => "running",
            Some(StopRequest::Pause) => "pausing",
            Some(_) => "dropping",
        };
        let t = format_duration(r.started.elapsed().unwrap_or_default());
        let res = format!("{}, {}w", fmt_ram(r.alloc.ram), r.alloc.workers);
        reply.out(&row(&r.job.id, state, &t, &res, &jobs::describe(&r.job.args)))?;
    }
    for (j, spot) in waiting {
        let (state, pos) = match spot {
            Spot::Queued(n) => ("queued", format!("#{}", n)),
            Spot::Express => ("waiting", "next".to_string()),
            Spot::Paused => ("paused", String::new()),
        };
        reply.out(&row(&j.id, state, &pos, "", &jobs::describe(&j.args)))?;
    }
    Ok(0)
}

/// Prints a job's log, following it until the job ends if it's still running.
fn log(d: &Daemon, reply: &mut Reply, key: &str) -> io::Result<i32> {
    if let Ok(id) = key.parse::<JobId>() {
        let waiting = d.lock().q.waiting().iter().find(|(j, _)| j.id == id).map(|(_, s)| match s {
            Spot::Queued(n) => format!("queued (#{} in line)", n),
            Spot::Express => "waiting for room".to_string(),
            Spot::Paused => "paused".to_string(),
        });
        // a paused job has a log from its earlier run, so fall through to it
        if let Some(w) = waiting.filter(|w| w != "paused") {
            reply.out(&format!("job {} is {}; its log starts when it runs", id, w))?;
            return Ok(0);
        }
    }
    let logs = jobs::find_logs(key);
    let Some(path) = logs.last() else {
        reply.err(&format!("no log found for '{}'", key))?;
        return Ok(1);
    };
    if logs.len() > 1 {
        reply.err(&format!("({} logs match '{}'; showing the latest)", logs.len(), key))?;
    }
    follow(d, reply, path)
}

/// Sends a log file, then keeps sending what's appended while its job runs.
fn follow(d: &Daemon, reply: &mut Reply, path: &Path) -> io::Result<i32> {
    let mut f = File::open(path)?;
    let mut buf = vec![0u8; 64 << 10];
    loop {
        // check before reading: the footer is written before the job leaves `running`
        let live = d.lock().q.running.iter().any(|r| r.log == *path);
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 { break; }
            reply.raw(&buf[..n])?;
        }
        if !live { return Ok(0); }
        thread::sleep(POLL);
    }
}

/// drop / pause / resume, each on a list of ids.
fn for_each_id(d: &Arc<Daemon>, reply: &mut Reply, verb: &str, ids: &[OsString]) -> io::Result<i32> {
    let mut code = 0;
    for a in ids {
        let Ok(id) = a.to_string_lossy().parse::<JobId>() else {
            reply.err(&format!("'{}' is not a job id", a.to_string_lossy()))?;
            code = 2;
            continue;
        };
        let mut st = d.lock();
        let running = st.q.running.iter_mut().find(|r| r.job.id == id);
        let msg: Result<String, String> = match (verb, running) {
            ("drop", Some(r)) => { r.stop = Some(StopRequest::Drop); Ok(format!("dropping job {}", id)) }
            ("pause", Some(r)) => { r.stop = Some(StopRequest::Pause); Ok(format!("pausing job {}", id)) }
            (_, Some(_)) => Err(format!("job {} is already running", id)), // resume
            ("drop", None) => match st.q.remove_waiting(id) {
                Some(_) => Ok(format!("job {} removed", id)),
                None => Err(format!("no running, queued or paused job {}", id)),
            },
            ("pause", None) => {
                if st.q.paused.iter().any(|j| j.id == id) {
                    Err(format!("job {} is already paused", id))
                } else if let Some(j) = st.q.remove_waiting(id) {
                    st.q.paused.push(j);
                    Ok(format!("job {} paused; it won't run until resumed", id))
                } else {
                    Err(format!("no running or queued job {}", id))
                }
            }
            (_, None) => if st.q.resume(id) {
                Ok(format!("job {} resumed; it runs as soon as there's room", id))
            } else {
                Err(format!("job {} isn't paused", id))
            },
        };
        save(&st.q);
        drop(st);
        match msg {
            Ok(m) => { event(&m); reply.out(&m)?; }
            Err(m) => { reply.err(&m)?; code = 1; }
        }
    }
    kick(d);
    Ok(code)
}

fn edit(d: &Arc<Daemon>, reply: &mut Reply, rest: &[OsString]) -> io::Result<i32> {
    let Some((id, tokens)) = rest.split_first() else {
        reply.err("usage: tard server edit <job id> [<input>] [<output>] [options]")?;
        return Ok(2);
    };
    let Ok(id) = id.to_string_lossy().parse::<JobId>() else {
        reply.err(&format!("'{}' is not a job id", id.to_string_lossy()))?;
        return Ok(2);
    };
    let mut st = d.lock();
    if st.q.running.iter().any(|r| r.job.id == id) {
        reply.err(&format!("job {} is running; pause it first, edit, then resume", id))?;
        return Ok(1);
    }
    let Some(job) = st.q.waiting_mut(id) else {
        reply.err(&format!("no queued or paused job {}", id))?;
        return Ok(1);
    };
    match jobs::edit_args(&job.args, tokens) {
        Ok(a) => {
            job.args = a;
            let m = format!("job {} is now: {}", id, jobs::describe(&job.args));
            save(&st.q);
            drop(st);
            event(&m);
            reply.out(&m)?;
            kick(d);
            Ok(0)
        }
        Err(e) => {
            reply.err(&format!("Error: {}", e))?;
            Ok(2)
        }
    }
}

/// Stops starting jobs, then either waits for running ones or stops them. Stopped jobs
/// go back where they were in the saved queue, with -c, so the next start resumes them.
fn stop(d: &Daemon, reply: &mut Reply, wait: bool) -> io::Result<()> {
    let mut st = d.lock();
    st.stopping = true;
    if wait && !st.q.running.is_empty() {
        let n = st.q.running.len();
        drop(st);
        reply.out(&format!("waiting for {} running job(s) to finish...", n))?;
        st = d.lock();
    } else if !wait {
        for r in st.q.running.iter_mut() {
            r.stop = Some(StopRequest::Requeue);
        }
    }
    let st = if wait {
        d.finished.wait_while(st, |st| !st.q.running.is_empty()).unwrap_or_else(|e| e.into_inner())
    } else {
        d.finished.wait_timeout_while(st, STOP_TIMEOUT, |st| !st.q.running.is_empty())
            .unwrap_or_else(|e| e.into_inner()).0
    };
    save(&st.q);
    let saved = st.q.saved().len();
    if saved > 0 {
        reply.out(&format!("server stopped; {} job(s) saved for the next start", saved))
    } else {
        reply.out("server stopped")
    }
}

// schedules

fn save_schedules(s: &[Schedule]) {
    if let Err(e) = schedule::save(s) {
        event(&format!("could not save schedules: {}", e));
    }
}

/// Submits scheduled runs when they come due (and, on startup, any that were missed
/// while the server was down, once each).
fn scheduler(d: Arc<Daemon>) {
    loop {
        let now = Zdt::now();
        let mut submitted = false;
        let mut st = d.lock();
        if !st.stopping {
            let keep = d.config.schedule.history;
            let State { schedules, q, next_id, .. } = &mut *st;
            let mut changed = false;
            for s in schedules.iter_mut() {
                if s.paused { continue; }
                let Some(due) = s.next_time().filter(|t| *t <= now) else { continue };
                changed = true;
                let at = schedule::show(&now);
                let mine = |j: &Job| j.schedule.as_deref() == Some(s.name.as_str());
                let pending = q.running.iter().any(|r| mine(&r.job)) || q.waiting().iter().any(|(j, _)| mine(j));
                if pending {
                    s.record(Run { at, job: 0, status: "skipped: previous run still going".into(), secs: 0 }, keep);
                    event(&format!("schedule {}: skipped a run, the previous one is still going", s.name));
                } else {
                    match jobs::parse_job(s.args.iter().map(OsString::from).collect()) {
                        Ok(args) => {
                            let id = *next_id;
                            *next_id += 1;
                            write_next_id(*next_id);
                            event(&format!("schedule {}: job {} submitted: {}", s.name, id, jobs::describe(&args)));
                            let job = Job { id, args, submitted: SystemTime::now(), schedule: Some(s.name.clone()) };
                            // scheduled runs start at their time, alongside the queue
                            q.submit(Chain { id, lane: Lane::Express, groups: [vec![job]].into() });
                            s.record(Run { at, job: id, status: "queued".into(), secs: 0 }, keep);
                            submitted = true;
                        }
                        Err(e) => {
                            event(&format!("schedule {}: run not started: {}", s.name, e));
                            s.record(Run { at, job: 0, status: format!("not run: {}", e), secs: 0 }, keep);
                        }
                    }
                }
                s.advance(&due, &now);
                if s.next.is_none() {
                    event(&format!("schedule {}: no runs left", s.name));
                }
            }
            if changed {
                save_schedules(schedules);
                save(q);
            }
        }
        // sleep until the next run is due, rechecking at least every minute (clock
        // changes, suspend) and whenever a schedule is added or edited
        let wait = st.schedules.iter().filter_map(|s| s.next_time())
            .map(|t| t.timestamp().as_second() - now.timestamp().as_second())
            .min()
            .map_or(60, |s| s.clamp(0, 60)) as u64;
        if submitted {
            drop(st);
            kick(&d);
            continue;
        }
        let _ = d.sched_wake.wait_timeout(st, Duration::from_secs(wait.max(1)));
    }
}

fn schedule_list(d: &Daemon, reply: &mut Reply) -> io::Result<i32> {
    let st = d.lock();
    if st.schedules.is_empty() {
        reply.out("no scheduled jobs (add one with: tard server schedule new <name>)")?;
        return Ok(0);
    }
    let row = |a: &str, b: &str, c: &str, e: &str| format!("{:<20} {:<17} {:<32} {}", a, b, c, e);
    reply.out(&row("NAME", "NEXT RUN", "REPEATS", "LAST RUN"))?;
    for s in &st.schedules {
        let next = s.next_time().map(|t| schedule::show(&t)).unwrap_or_else(|| "(done)".into());
        let last = s.history.last().map(|h| run_status(&st.q, h)).unwrap_or_else(|| "-".into());
        reply.out(&row(&s.name, &next, &s.regularity(), &last))?;
    }
    Ok(0)
}

/// "ok in 4m12s (job 31)", "running (job 32)", "skipped: ..."
fn run_status(q: &Queue, h: &Run) -> String {
    if h.job == 0 {
        return h.status.clone();
    }
    let status = if h.status == "queued" && q.running.iter().any(|r| r.job.id == h.job) { "running" } else { h.status.as_str() };
    let took = if h.secs > 0 || status == "ok" { format!(" in {}", format_duration(Duration::from_secs(h.secs))) } else { String::new() };
    format!("{}{} (job {})", status, took, h.job)
}

fn find_schedule<'a>(st: &'a State, reply: &mut Reply, name: &str) -> io::Result<Option<&'a Schedule>> {
    let s = st.schedules.iter().find(|s| s.name == name);
    if s.is_none() {
        reply.err(&format!("no scheduled job '{}' (see tard server schedule)", name))?;
    }
    Ok(s)
}

fn schedule_show(d: &Daemon, reply: &mut Reply, name: &str) -> io::Result<i32> {
    let st = d.lock();
    let Some(s) = find_schedule(&st, reply, name)? else { return Ok(1) };
    reply.out(&s.name)?;
    reply.out(&format!("  job:      {}", schedule::shell_join(&s.args)))?;
    reply.out(&format!("  repeats:  {}", s.regularity()))?;
    let next = match s.next_time() {
        Some(t) => {
            let secs = (t.timestamp().as_second() - Zdt::now().timestamp().as_second()).max(0) as u64;
            format!("{} (in {})", schedule::show(&t), format_duration(Duration::from_secs(secs)))
        }
        None => "none, all runs done".into(),
    };
    reply.out(&format!("  next run: {}", next))?;
    reply.out(&format!("  logs:     {}", jobs::scheduled_log_dir(&s.name).display()))?;
    if s.history.is_empty() {
        reply.out("  no runs yet")?;
    } else {
        reply.out(&format!("  last {} run(s):", s.history.len()))?;
        for h in s.history.iter().rev() {
            reply.out(&format!("    {}  {}", h.at, run_status(&st.q, h)))?;
        }
    }
    Ok(0)
}

/// Fields for the client's edit prompts, one per line: repeat, runs ("forever" or a
/// count), next run ("YYYY-MM-DD HH:MM" or empty), and the job's arguments.
fn schedule_get(d: &Daemon, reply: &mut Reply, name: &str) -> io::Result<i32> {
    let st = d.lock();
    let Some(s) = find_schedule(&st, reply, name)? else { return Ok(1) };
    reply.out(&s.repeat)?;
    reply.out(&s.runs.map_or("forever".to_string(), |n| n.to_string()))?;
    reply.out(&s.next_time().map(|t| schedule::show(&t)).unwrap_or_default())?;
    reply.out(&schedule::shell_join(&s.args))?;
    Ok(0)
}

/// schedule-add / schedule-set <name> <repeat> <runs|forever> <first run> <job args...>
fn schedule_put(d: &Daemon, reply: &mut Reply, replace: bool, rest: &[OsString]) -> io::Result<i32> {
    let parsed = (|| -> Result<Schedule, String> {
        let [name, repeat, runs, first, argv @ ..] = rest else {
            return Err("usage: schedule-add <name> <repeat> <runs|forever> <first run> <job args>".into());
        };
        let name = name.to_string_lossy().into_owned();
        schedule::check_name(&name)?;
        let repeat = schedule::parse_repeat(&repeat.to_string_lossy())?;
        let runs = match runs.to_string_lossy().as_ref() {
            "forever" if repeat != schedule::Repeat::Once => None,
            "forever" => Some(1),
            n => Some(n.parse::<u32>().ok().filter(|n| *n > 0).ok_or("runs must be a positive number or forever")?),
        };
        let runs = if repeat == schedule::Repeat::Once { Some(1) } else { runs };
        let first = schedule::parse_zdt(&first.to_string_lossy()).ok_or("bad first-run time")?;
        let args = jobs::parse_job(argv.to_vec())?;
        let args = args.to_argv().into_iter()
            .map(|a| a.into_string().map_err(|_| "paths that aren't valid UTF-8 can't be scheduled".to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Schedule { name, args, repeat: repeat.to_text(), runs, done: 0, next: Some(first.to_string()), history: Vec::new(), paused: false })
    })();
    let mut new = match parsed {
        Ok(s) => s,
        Err(e) => {
            reply.err(&format!("Error: {}", e))?;
            return Ok(2);
        }
    };
    let mut st = d.lock();
    let existing = st.schedules.iter().position(|s| s.name == new.name);
    match (existing, replace) {
        (Some(_), false) => {
            reply.err(&format!("a scheduled job named '{}' already exists (change it with: tard server schedule edit {})", new.name, new.name))?;
            return Ok(1);
        }
        (None, true) => {
            reply.err(&format!("no scheduled job '{}'", new.name))?;
            return Ok(1);
        }
        (Some(i), true) => {
            let old = st.schedules.remove(i);
            new.done = old.done;
            new.history = old.history;
            new.paused = old.paused;
            if new.runs.is_some_and(|n| new.done >= n) {
                new.next = None;
            }
            st.schedules.insert(i, new.clone());
        }
        (None, false) => st.schedules.push(new.clone()),
    }
    save_schedules(&st.schedules);
    drop(st);
    d.sched_wake.notify_all();
    let verb = if replace { "updated" } else { "added" };
    let next = new.next_time().map(|t| schedule::show(&t)).unwrap_or_else(|| "none".into());
    event(&format!("schedule {} {}: {} ({}), next run {}", new.name, verb, schedule::shell_join(&new.args), new.regularity(), next));
    reply.out(&format!("scheduled job '{}' {}; next run {}", new.name, verb, next))?;
    Ok(0)
}

fn schedule_rm(d: &Daemon, reply: &mut Reply, name: &str) -> io::Result<i32> {
    let mut st = d.lock();
    let Some(i) = st.schedules.iter().position(|s| s.name == name) else {
        reply.err(&format!("no scheduled job '{}'", name))?;
        return Ok(1);
    };
    st.schedules.remove(i);
    // runs it already submitted but that haven't started go too; a running one finishes
    let waiting: Vec<JobId> = st.q.waiting().iter()
        .filter(|(j, _)| j.schedule.as_deref() == Some(name))
        .map(|(j, _)| j.id)
        .collect();
    for id in &waiting {
        st.q.remove_waiting(*id);
    }
    save_schedules(&st.schedules);
    save(&st.q);
    drop(st);
    event(&format!("schedule {} removed", name));
    reply.out(&format!("scheduled job '{}' removed (its logs are kept in {})", name, jobs::scheduled_log_dir(name).display()))?;
    Ok(0)
}

fn schedule_pause(d: &Daemon, reply: &mut Reply, name: &str, pause: bool) -> io::Result<i32> {
    let mut st = d.lock();
    let Some(s) = st.schedules.iter_mut().find(|s| s.name == name) else {
        reply.err(&format!("no scheduled job '{}'", name))?;
        return Ok(1);
    };
    if s.paused == pause {
        reply.err(&format!("scheduled job '{}' is already {}", name, if pause { "paused" } else { "running" }))?;
        return Ok(1);
    }
    s.paused = pause;
    let msg = if pause {
        format!("scheduled job '{}' paused; it won't run until resumed", name)
    } else {
        match s.next_time() {
            Some(t) if t <= Zdt::now() => format!("scheduled job '{}' resumed; the run that came due while paused starts now", name),
            Some(t) => format!("scheduled job '{}' resumed; next run {}", name, schedule::show(&t)),
            None => format!("scheduled job '{}' resumed, but it has no runs left", name),
        }
    };
    save_schedules(&st.schedules);
    drop(st);
    d.sched_wake.notify_all();
    event(&msg);
    reply.out(&msg)?;
    Ok(0)
}

fn schedule_log(d: &Daemon, reply: &mut Reply, name: &str) -> io::Result<i32> {
    let logs = jobs::list_logs(&jobs::scheduled_log_dir(name));
    let Some(path) = logs.last() else {
        reply.err(&format!("no logs for scheduled job '{}'", name))?;
        return Ok(1);
    };
    if logs.len() > 1 {
        reply.err(&format!("(latest of {} runs; older logs are in {})", logs.len(), jobs::scheduled_log_dir(name).display()))?;
    }
    follow(d, reply, path)
}

fn shutdown() -> ! {
    let _ = fs::remove_file(token_path());
    let _ = fs::remove_file(pid_path());
    event("server stopped");
    let _ = io::stdout().flush();
    std::process::exit(0);
}

/// Response stream to one client.
struct Reply<'a>(BufWriter<&'a TcpStream>);

impl Reply<'_> {
    fn out(&mut self, line: &str) -> io::Result<()> {
        proto::write_frame(&mut self.0, &Frame::Out(format!("{}\n", line).into_bytes()))
    }
    fn err(&mut self, line: &str) -> io::Result<()> {
        proto::write_frame(&mut self.0, &Frame::Err(format!("{}\n", line).into_bytes()))
    }
    fn raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        proto::write_frame(&mut self.0, &Frame::Out(bytes.to_vec()))
    }
    fn exit(&mut self, code: i32) -> io::Result<()> {
        proto::write_frame(&mut self.0, &Frame::Exit(code))
    }
}

fn next_id_path() -> PathBuf {
    server_dir().join("next_id")
}

fn read_next_id() -> JobId {
    fs::read_to_string(next_id_path()).ok().and_then(|s| s.trim().parse().ok()).unwrap_or(1)
}

fn write_next_id(id: JobId) {
    let _ = fs::write(next_id_path(), id.to_string());
}

fn write_private(path: &Path, contents: &[u8]) -> Result<(), String> {
    create_private_file(path)
        .and_then(|mut f| f.write_all(contents))
        .map_err(|e| format!("could not write {}: {}", path.display(), e))
}

/// 32 random hex chars: /dev/urandom where there is one, else std's per-process hash keys.
fn new_token() -> Vec<u8> {
    let mut raw = [0u8; 16];
    let from_os = File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut raw))
        .is_ok();
    if !from_os {
        use std::hash::{BuildHasher, Hasher};
        for half in raw.chunks_mut(8) {
            let mut h = std::collections::hash_map::RandomState::new().build_hasher();
            h.write_u128(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos());
            half.copy_from_slice(&h.finish().to_le_bytes());
        }
    }
    raw.iter().map(|b| format!("{:02x}", b)).collect::<String>().into_bytes()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
