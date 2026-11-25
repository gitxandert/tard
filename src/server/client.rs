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
use crate::server::{create_private_dir, daemon, server_dir, server_log_path, token_path};
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
        Some("stop") => match send(&config, &args) {
            Err(Unreachable) => {
                eprintln!("server is not running");
                1
            }
            Ok(code) => code,
        },
        Some(_) => match send(&config, &args) {
            Err(Unreachable) => {
                eprintln!("server is not running (start it with: tard server start)");
                1
            }
            Ok(code) => code,
        },
    }
}

/// The server didn't answer on its port.
struct Unreachable;

fn start(config: &Config) -> i32 {
    let ping = [OsString::from("ping")];
    if let Ok(code) = send(config, &ping) {
        return code; // already running: ping printed its status
    }
    if let Err(e) = spawn_daemon() {
        eprintln!("Error: could not start server: {}", e);
        return 1;
    }
    let deadline = Instant::now() + START_TIMEOUT;
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
        // a stale token can be read before the new server writes its own; retry until it answers
        if let Ok(0) = request(config, &ping, &mut io::sink(), &mut io::sink()) {
            return send(config, &ping).unwrap_or(1);
        }
    }
    eprintln!("Error: server did not come up within {}s. Last lines of {}:", START_TIMEOUT.as_secs(), server_log_path().display());
    print_tail(&server_log_path(), 5);
    1
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
        const DETACHED_PROCESS: u32 = 0x8;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x200;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
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

Run tard jobs from a background server. Settings live in {}.

Commands:
  start      Start the server in the background (also: bare `tard server`)
  stop       Stop the server
  run        Run the server in the foreground (for systemd and debugging)
  ping       Show whether the server is running
  -h, --help Show this help

Server output is appended to {}.",
        crate::config::config_path().display(),
        server_log_path().display(),
    );
}
