// The long-running server: `tard server run` (start spawns it detached).
use std::{
    ffi::OsString,
    fs,
    io::{self, BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::{Duration, SystemTime},
};
use crate::config::Config;
use crate::server::proto::{self, Frame};
use crate::server::{create_private_dir, create_private_file, pid_path, server_dir, token_path};
use crate::utils::logger::format_iso_time;
use crate::utils::path::bytes_to_path;

struct Daemon {
    token: Vec<u8>,
    config: Config,
    started: SystemTime,
}

/// Runs the server in the foreground until `tard server stop`.
pub fn run() -> Result<(), String> {
    let config = Config::load()?;
    let dir = server_dir();
    create_private_dir(&dir).map_err(|e| format!("could not create {}: {}", dir.display(), e))?;

    // bind before touching the token, so a second `start` can't clobber a live server's token
    let port = config.server.port;
    let listener = TcpListener::bind(("127.0.0.1", port))
        .map_err(|e| format!("could not listen on 127.0.0.1:{}: {}", port, e))?;

    let token = new_token();
    write_private(&token_path(), &token)?;
    write_private(&pid_path(), std::process::id().to_string().as_bytes())?;

    let daemon = Arc::new(Daemon { token, config, started: SystemTime::now() });
    event(&format!("server started on 127.0.0.1:{} (pid {})", port, std::process::id()));

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let d = Arc::clone(&daemon);
                thread::spawn(move || {
                    if let Err(e) = handle(&d, s) {
                        event(&format!("connection error: {}", e));
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

    match args.first().and_then(|a| a.to_str()) {
        Some("ping") => {
            let up = d.started.elapsed().unwrap_or_default().as_secs();
            reply.out(&format!(
                "server running on 127.0.0.1:{} (pid {}), up {}h{:02}m",
                d.config.server.port, std::process::id(), up / 3600, up / 60 % 60,
            ))?;
            reply.exit(0)
        }
        Some("stop") => {
            reply.out("server stopped")?;
            reply.exit(0)?;
            shutdown()
        }
        Some(other) => {
            reply.err(&format!("unknown server command '{}' (see tard server -h)", other))?;
            reply.exit(2)
        }
        None => {
            reply.err("empty request")?;
            reply.exit(2)
        }
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
    fn exit(&mut self, code: i32) -> io::Result<()> {
        proto::write_frame(&mut self.0, &Frame::Exit(code))
    }
}

fn write_private(path: &std::path::Path, contents: &[u8]) -> Result<(), String> {
    create_private_file(path)
        .and_then(|mut f| f.write_all(contents))
        .map_err(|e| format!("could not write {}: {}", path.display(), e))
}

/// 32 random hex chars: /dev/urandom where there is one, else std's per-process hash keys.
fn new_token() -> Vec<u8> {
    let mut raw = [0u8; 16];
    let from_os = fs::File::open("/dev/urandom")
        .and_then(|mut f| io::Read::read_exact(&mut f, &mut raw))
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
