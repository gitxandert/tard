use std::{
    fs::File,
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crate::utils::formatting::format_size;

const DRAIN_INTERVAL: Duration = Duration::from_millis(5);
const CSV_HEADER: &str = "filename,directory,size_bytes,size_human-readable,lmod_time\n";

/// One processed file. `path` is absolute; the logger strips its root before writing.
pub struct LogEntry {
    pub path: PathBuf,
    pub size: u64,
    pub mtime: SystemTime,
}

pub enum LogMsg {
    /// CSV row in the log file; "<path> (<size>)" on stdout with -v
    Entry(LogEntry),
    /// always shown on stdout (start/finish lines)
    Info(String),
    /// shown on stdout only with -v
    Note(String),
    /// always shown, on stderr (errors and warnings)
    Error(String),
    /// shown on stderr, then the logger flushes everything and exits the process
    Fatal(String),
}

pub type LogTx = Sender<LogMsg>;

pub struct Logger {
    pub tx: LogTx,
    handle: JoinHandle<io::Result<()>>,
    pub log_path: PathBuf,
}

impl Logger {
    /// Picks the destinations once, before any worker starts: always the CSV file at
    /// `log_path` (its directory is created if needed), plus stdout when `verbose`.
    /// Entry paths are written relative to `strip_root`.
    pub fn spawn(verbose: bool, log_path: PathBuf, strip_root: PathBuf) -> io::Result<Self> {
        let (tx, rx) = crossbeam_channel::unbounded::<LogMsg>();
        if let Some(dir) = log_path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir).map_err(|e| {
                io::Error::new(e.kind(), format!("could not create log directory {}: {}", dir.display(), e))
            })?;
        }
        let file = File::create(&log_path).map_err(|e| {
            io::Error::new(e.kind(), format!("could not create log {}: {}", log_path.display(), e))
        })?;
        let file = BufWriter::with_capacity(1 << 16, file);
        // not stdout().lock(): holding the lock would deadlock main-thread println!s
        let out = BufWriter::new(io::stdout());
        let handle = if verbose {
            thread::spawn(move || run::<true>(file, out, rx, &strip_root))
        } else {
            thread::spawn(move || run::<false>(file, out, rx, &strip_root))
        };
        Ok(Self { tx, handle, log_path })
    }

    /// Drops this sender and waits for the logger to drain. Every other `LogTx`
    /// clone must already be dropped, or this blocks.
    pub fn finish(self) {
        drop(self.tx);
        // a logger I/O failure was already reported on stderr by the logger itself
        let _ = self.handle.join().expect("Logger thread panicked");
    }

    /// Reports `msg` through the logger, which flushes and exits with status 1.
    /// For unrecoverable errors while workers still hold senders.
    pub fn fatal(&self, msg: String) -> ! {
        if let Err(e) = self.tx.send(LogMsg::Fatal(msg)) {
            // logger already gone: nothing else can print this
            let LogMsg::Fatal(m) = e.into_inner() else { unreachable!() };
            eprintln!("{}", m);
            std::process::exit(1);
        }
        loop { thread::park(); }
    }
}

fn run<const VERBOSE: bool>(
    file: BufWriter<File>,
    out: BufWriter<io::Stdout>,
    rx: Receiver<LogMsg>,
    strip_root: &Path,
) -> io::Result<()> {
    let r = drain::<VERBOSE>(file, out, rx, strip_root);
    if let Err(ref e) = r {
        eprintln!("Err -- could not write log: {}", e);
    }
    r
}

// VERBOSE is a compile-time choice, so the logger has no runtime branch on it either.
fn drain<const VERBOSE: bool>(
    mut file: BufWriter<File>,
    mut out: BufWriter<io::Stdout>,
    rx: Receiver<LogMsg>,
    strip_root: &Path,
) -> io::Result<()> {
    file.write_all(CSV_HEADER.as_bytes())?;
    let mut line = Vec::<u8>::with_capacity(256);
    // Drain on a timer instead of blocking in recv(): a parked receiver makes every
    // send() pay for a futex wake, which showed up as ~0.3s of extra CPU per 20k files.
    // With nobody parked, send() is just a lock-free push.
    loop {
        loop {
            match rx.try_recv() {
                Ok(LogMsg::Entry(e)) => {
                    let rel = e.path.strip_prefix(strip_root).unwrap_or(&e.path);
                    line.clear();
                    write_csv_row(&mut line, rel, &e)?;
                    file.write_all(&line)?;
                    if VERBOSE { writeln!(out, "{} ({})", rel.display(), format_size(e.size))?; }
                }
                Ok(LogMsg::Info(s)) => writeln!(out, "{}", s)?,
                Ok(LogMsg::Note(s)) => if VERBOSE { writeln!(out, "{}", s)?; },
                Ok(LogMsg::Error(s)) => {
                    out.flush()?; // keep stdout/stderr in order
                    eprintln!("{}", s);
                }
                Ok(LogMsg::Fatal(s)) => {
                    out.flush()?;
                    file.flush()?;
                    eprintln!("{}", s);
                    std::process::exit(1);
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    out.flush()?;
                    return file.flush();
                }
            }
        }
        // stdout: flush each round so output stays live (no-op when empty). file: keep batching.
        out.flush()?;
        thread::sleep(DRAIN_INTERVAL);
    }
}

fn write_csv_row<W: Write>(w: &mut W, rel: &Path, e: &LogEntry) -> io::Result<()> {
    let name = rel.file_name().map(|n| n.to_string_lossy()).unwrap_or_default();
    let dir = rel.parent().map(|p| p.to_string_lossy()).unwrap_or_default();
    write_csv_field(w, &name)?;
    w.write_all(b",")?;
    write_csv_field(w, &dir)?;
    writeln!(w, ",{},{},{}", e.size, format_size(e.size), format_iso_time(e.mtime))
}

fn write_csv_field<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    if s.bytes().any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r')) {
        w.write_all(b"\"")?;
        w.write_all(s.replace('"', "\"\"").as_bytes())?;
        w.write_all(b"\"")
    } else {
        w.write_all(s.as_bytes())
    }
}

// UTC civil time from a SystemTime (Howard Hinnant's days-from-civil inverse).
fn civil(t: SystemTime) -> (i64, u32, u32, u32, u32, u32) {
    let secs = match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    };
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400) as u32;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d, sod / 3600, sod / 60 % 60, sod % 60)
}

/// e.g. 2026-11-23T19:18:05Z
pub fn format_iso_time(t: SystemTime) -> String {
    let (y, mo, d, h, mi, s) = civil(t);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, mi, s)
}

/// e.g. 2026-11-23_19-18-05 (UTC, filename-safe)
pub fn format_filename_time(t: SystemTime) -> String {
    let (y, mo, d, h, mi, s) = civil(t);
    format!("{:04}-{:02}-{:02}_{:02}-{:02}-{:02}", y, mo, d, h, mi, s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn civil_dates() {
        assert_eq!(format_iso_time(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        // 2000-02-29T12:34:56Z
        assert_eq!(format_iso_time(UNIX_EPOCH + Duration::from_secs(951_827_696)), "2000-02-29T12:34:56Z");
        // 2026-11-23T00:00:00Z
        assert_eq!(format_filename_time(UNIX_EPOCH + Duration::from_secs(1_795_392_000)), "2026-11-23_00-00-00");
    }

    #[test]
    fn csv_quoting() {
        let mut out = Vec::new();
        write_csv_field(&mut out, "a,\"b\"").unwrap();
        assert_eq!(out, b"\"a,\"\"b\"\"\"");
    }
}
