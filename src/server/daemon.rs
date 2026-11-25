// The long-running server: `tard server run` (start spawns it detached).
use std::{
    collections::VecDeque,
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
use crate::config::Config;
use crate::server::jobs::{self, Job, JobId};
use crate::server::proto::{self, Frame};
use crate::server::{create_private_dir, create_private_file, pid_path, server_dir, token_path};
use crate::utils::formatting::format_duration;
use crate::utils::logger::format_iso_time;
use crate::utils::path::bytes_to_path;

const POLL: Duration = Duration::from_millis(200);
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

struct Daemon {
    token: Vec<u8>,
    config: Config,
    started: SystemTime,
    exe: PathBuf, // this binary, run for each job
    state: Mutex<State>,
    /// signalled whenever the queue or the running set changes
    wake: Condvar,
}

struct State {
    next_id: JobId,
    queue: VecDeque<Job>,
    running: Vec<Running>,
    stopping: bool,
}

struct Running {
    job: Job,
    log: PathBuf,
    started: SystemTime,
    drop_requested: bool,
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
    event(&format!("server started on 127.0.0.1:{} (pid {})", port, std::process::id()));

    let (queue, bad) = jobs::load_queue();
    for (id, why) in bad {
        event(&format!("job {} dropped from the saved queue: {}", id, why));
    }
    if !queue.is_empty() {
        event(&format!("restored {} queued job(s)", queue.len()));
    }
    let next_id = read_next_id()
        .max(jobs::max_logged_id(&jobs::logs_dir()) + 1)
        .max(queue.iter().map(|j| j.id + 1).max().unwrap_or(1));

    let daemon = Arc::new(Daemon {
        token,
        config,
        started: SystemTime::now(),
        exe,
        state: Mutex::new(State { next_id, queue: queue.into(), running: Vec::new(), stopping: false }),
        wake: Condvar::new(),
    });
    let _ = jobs::save_queue(&daemon.lock().queue); // drops any jobs that failed to load

    let d = Arc::clone(&daemon);
    thread::spawn(move || dispatch(d));

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

/// One timestamped line of server output (stdout, which `start` points at server.log).
pub fn event(msg: &str) {
    println!("{} {}", format_iso_time(SystemTime::now()), msg);
}

impl Daemon {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn max_running(&self) -> usize {
        1 // jobs run one at a time until parallel jobs land
    }
}

// running jobs

/// Starts queued jobs as room frees up.
fn dispatch(d: Arc<Daemon>) {
    loop {
        let (job, log) = {
            let mut st = d.lock();
            while st.stopping || st.queue.is_empty() || st.running.len() >= d.max_running() {
                st = d.wake.wait(st).unwrap_or_else(|e| e.into_inner());
            }
            let job = st.queue.pop_front().unwrap();
            let _ = jobs::save_queue(&st.queue);
            let started = SystemTime::now();
            let log = jobs::new_log_path(&jobs::logs_dir(), job.id, &job.args, started);
            // moved to running under the same lock, so `ls` and `drop` always see it
            st.running.push(Running { job: job.clone(), log: log.clone(), started, drop_requested: false });
            (job, log)
        };
        let started = SystemTime::now();
        match spawn_job(&d, &job, &log) {
            Ok(child) => {
                event(&format!("job {} started: {}", job.id, jobs::describe(&job.args)));
                let d = Arc::clone(&d);
                thread::spawn(move || supervise(d, job.id, child, log, started));
            }
            Err(e) => {
                append(&log, &format!("--- could not start: {}\n", e));
                event(&format!("job {} could not start: {}", job.id, e));
                finish(&d, job.id, &log);
            }
        }
    }
}

fn spawn_job(d: &Daemon, job: &Job, log: &Path) -> io::Result<Child> {
    let mut f = create_private_file(log)?;
    let argv = job.args.to_argv();
    writeln!(f, "job {}: {}", job.id, jobs::describe(&job.args))?;
    writeln!(f, "command: tard {}", argv.iter().map(|a| a.to_string_lossy()).collect::<Vec<_>>().join(" "))?;
    writeln!(f, "submitted {}", format_iso_time(job.submitted))?;
    writeln!(f, "started   {}", format_iso_time(SystemTime::now()))?;
    writeln!(f, "---")?;
    Command::new(&d.exe)
        .args(&argv)
        .stdin(Stdio::null())
        .stdout(f.try_clone()?)
        .stderr(f)
        .current_dir(server_dir())
        .spawn()
}

enum Outcome {
    Exited(ExitStatus),
    Dropped,
    TimedOut(Duration),
    Lost(io::Error),
}

/// Waits for one job, killing it if it's dropped or runs past max_run_time.
fn supervise(d: Arc<Daemon>, id: JobId, mut child: Child, log: PathBuf, started: SystemTime) {
    let max = d.config.server.max_run_time;
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Outcome::Exited(status),
            Err(e) => break Outcome::Lost(e),
            Ok(None) => (),
        }
        let dropped = d.lock().running.iter().any(|r| r.job.id == id && r.drop_requested);
        if dropped {
            let _ = child.kill();
            let _ = child.wait();
            break Outcome::Dropped;
        }
        if let Some(m) = max.filter(|m| started.elapsed().unwrap_or_default() > *m) {
            let _ = child.kill();
            let _ = child.wait();
            break Outcome::TimedOut(m);
        }
        thread::sleep(POLL);
    };

    let took = format_duration(started.elapsed().unwrap_or_default());
    let summary = match outcome {
        Outcome::Exited(s) if s.success() => format!("finished in {}", took),
        Outcome::Exited(s) => match s.code() {
            Some(c) => format!("failed (exit {}) after {}", c, took),
            None => format!("failed (killed by a signal) after {}", took),
        },
        Outcome::Dropped => format!("dropped after {}", took),
        Outcome::TimedOut(m) => format!("dropped: ran past max_run_time ({})", format_duration(m)),
        Outcome::Lost(e) => format!("lost track of the process: {}", e),
    };
    append(&log, &format!("--- {} ({})\n", summary, format_iso_time(SystemTime::now())));
    event(&format!("job {} {}", id, summary));
    finish(&d, id, &log);
}

/// Takes a job out of `running` once its log is complete, and lets the next one start.
fn finish(d: &Daemon, id: JobId, log: &Path) {
    let mut st = d.lock();
    st.running.retain(|r| r.job.id != id);
    let live: Vec<&Path> = st.running.iter().map(|r| r.log.as_path()).collect();
    jobs::prune_logs(log.parent().unwrap_or(Path::new(".")), d.config.logs.keep, &live);
    drop(st);
    d.wake.notify_all();
}

fn append(log: &Path, text: &str) {
    if let Ok(mut f) = OpenOptions::new().append(true).open(log) {
        let _ = f.write_all(text.as_bytes());
    }
}

// requests

fn handle(d: &Daemon, stream: TcpStream) -> io::Result<()> {
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
        "drop" if !rest.is_empty() => drop_jobs(d, &mut reply, rest)?,
        "drop" => { reply.err("usage: tard server drop <job id>...")?; 2 }
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

fn submit(d: &Daemon, reply: &mut Reply, argv: Vec<OsString>) -> io::Result<i32> {
    let args = match jobs::parse_job(argv) {
        Ok(a) => a,
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
    let id = st.next_id;
    st.next_id += 1;
    write_next_id(st.next_id);
    let ahead = st.running.len() + st.queue.len();
    let room = st.running.len() < d.max_running() && st.queue.is_empty();
    let desc = jobs::describe(&args);
    st.queue.push_back(Job { id, args, submitted: SystemTime::now() });
    if let Err(e) = jobs::save_queue(&st.queue) {
        event(&format!("could not save the queue: {}", e));
    }
    drop(st);
    d.wake.notify_all();

    event(&format!("job {} submitted: {}", id, desc));
    if room {
        reply.out(&format!("job {} started: {}", id, desc))?;
    } else {
        reply.out(&format!("job {} queued ({} ahead): {}", id, ahead, desc))?;
    }
    reply.out(&format!("  follow it with: tard server log {}", id))?;
    Ok(0)
}

fn ls(d: &Daemon, reply: &mut Reply) -> io::Result<i32> {
    let st = d.lock();
    if st.running.is_empty() && st.queue.is_empty() {
        reply.out("no jobs running or queued")?;
        return Ok(0);
    }
    reply.out(&format!("{:>4}  {:<8} {:>7}  {}", "ID", "STATE", "TIME", "JOB"))?;
    for r in &st.running {
        let state = if r.drop_requested { "dropping" } else { "running" };
        let t = format_duration(r.started.elapsed().unwrap_or_default());
        reply.out(&format!("{:>4}  {:<8} {:>7}  {}", r.job.id, state, t, jobs::describe(&r.job.args)))?;
    }
    for (i, j) in st.queue.iter().enumerate() {
        reply.out(&format!("{:>4}  {:<8} {:>7}  {}", j.id, "queued", format!("#{}", i + 1), jobs::describe(&j.args)))?;
    }
    Ok(0)
}

/// Prints a job's log, following it until the job ends if it's still running.
fn log(d: &Daemon, reply: &mut Reply, key: &str) -> io::Result<i32> {
    if let Ok(id) = key.parse::<JobId>() {
        let queued = d.lock().queue.iter().position(|j| j.id == id);
        if let Some(pos) = queued {
            reply.out(&format!("job {} is queued (#{} in line); its log starts when it runs", id, pos + 1))?;
            return Ok(0);
        }
    }
    let logs = jobs::find_logs(&jobs::logs_dir(), key);
    let Some(path) = logs.last() else {
        reply.err(&format!("no log found for '{}'", key))?;
        return Ok(1);
    };
    if logs.len() > 1 {
        reply.err(&format!("({} logs match '{}'; showing the latest)", logs.len(), key))?;
    }
    let mut f = File::open(path)?;
    let mut buf = vec![0u8; 64 << 10];
    loop {
        // check before reading: the footer is written before the job leaves `running`
        let live = d.lock().running.iter().any(|r| r.log == *path);
        loop {
            let n = f.read(&mut buf)?;
            if n == 0 { break; }
            reply.raw(&buf[..n])?;
        }
        if !live { return Ok(0); }
        thread::sleep(POLL);
    }
}

fn drop_jobs(d: &Daemon, reply: &mut Reply, ids: &[OsString]) -> io::Result<i32> {
    let mut code = 0;
    for a in ids {
        let Ok(id) = a.to_string_lossy().parse::<JobId>() else {
            reply.err(&format!("'{}' is not a job id", a.to_string_lossy()))?;
            code = 2;
            continue;
        };
        let mut st = d.lock();
        if let Some(r) = st.running.iter_mut().find(|r| r.job.id == id) {
            r.drop_requested = true;
            drop(st);
            reply.out(&format!("dropping job {}", id))?;
        } else if let Some(pos) = st.queue.iter().position(|j| j.id == id) {
            st.queue.remove(pos);
            let _ = jobs::save_queue(&st.queue);
            drop(st);
            event(&format!("job {} removed from the queue", id));
            reply.out(&format!("job {} removed from the queue", id))?;
        } else {
            reply.err(&format!("no running or queued job {}", id))?;
            code = 1;
        }
    }
    Ok(code)
}

/// Stops starting jobs, then either waits for running ones or drops them. Dropped jobs
/// go back to the front of the saved queue with -c, so the next start resumes them.
fn stop(d: &Daemon, reply: &mut Reply, wait: bool) -> io::Result<()> {
    let mut st = d.lock();
    st.stopping = true;
    if wait && !st.running.is_empty() {
        let n = st.running.len();
        drop(st);
        reply.out(&format!("waiting for {} running job(s) to finish...", n))?;
        st = d.lock();
    } else if !wait {
        let mut resumed: Vec<Job> = Vec::new();
        for r in st.running.iter_mut() {
            r.drop_requested = true;
            let mut j = r.job.clone();
            j.args.resume = true;
            resumed.push(j);
        }
        for j in resumed.into_iter().rev() {
            st.queue.push_front(j);
        }
        let _ = jobs::save_queue(&st.queue);
    }
    let st = if wait {
        d.wake.wait_while(st, |st| !st.running.is_empty()).unwrap_or_else(|e| e.into_inner())
    } else {
        d.wake.wait_timeout_while(st, STOP_TIMEOUT, |st| !st.running.is_empty())
            .unwrap_or_else(|e| e.into_inner()).0
    };
    let saved = st.queue.len();
    if saved > 0 {
        reply.out(&format!("server stopped; {} job(s) saved and will resume on next start", saved))
    } else {
        reply.out("server stopped")
    }
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
