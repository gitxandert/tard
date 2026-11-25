// Jobs as the server sees them, and their log files:
// ~/.tard/server/logs/<YYYY-MM-DD_HH-MM-SS>_<id>_<input name>.log (UTC)
use std::{
    ffi::OsString,
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crate::cli::{Args, Cmd, parse_args_from};
use crate::server::server_dir;
use crate::utils::formatting::format_size;
use crate::utils::logger::format_filename_time;
use crate::utils::path::{bytes_to_path, path_to_bytes};

pub type JobId = u64;

#[derive(Clone)]
pub struct Job {
    pub id: JobId,
    pub args: Args,
    pub submitted: SystemTime,
}

pub fn logs_dir() -> PathBuf {
    server_dir().join("logs")
}

/// Parses and checks one job's arguments (as typed after `tard server`).
pub fn parse_job(argv: Vec<OsString>) -> Result<Args, String> {
    let args = parse_args_from(argv)?;
    validate(&args)?;
    Ok(args)
}

fn validate(a: &Args) -> Result<(), String> {
    if a.cmd == Cmd::Help {
        return Err("-h isn't a job; see tard -h".into());
    }
    let input = a.input_dir.as_deref().ok_or("no input given")?;
    let output = a.output_dir.as_deref().ok_or("no output directory given")?;
    match a.cmd {
        Cmd::Archive if !input.is_dir() => return Err(format!("{} is not a directory", input.display())),
        Cmd::Extract if input.extension().is_none_or(|e| e != "tard") => {
            return Err(format!("{} is not a .tard file", input.display()));
        }
        _ => (),
    }
    if !output.is_dir() {
        return Err(format!("{} is not a directory", output.display()));
    }
    Ok(())
}

/// One line for `ls` and the server log, e.g. "archive /data/photos -> /backup (-r 64MB)".
pub fn describe(a: &Args) -> String {
    let verb = if a.cmd == Cmd::Extract { "extract" } else { "archive" };
    let mut s = format!(
        "{} {} -> {}",
        verb,
        a.input_dir.as_deref().map(|p| p.display().to_string()).unwrap_or_default(),
        a.output_dir.as_deref().map(|p| p.display().to_string()).unwrap_or_default(),
    );
    let mut flags = Vec::new();
    if a.resume { flags.push("-c".to_string()); }
    if a.verbose { flags.push("-v".to_string()); }
    if a.ram_given { flags.push(format!("-r {}", format_size(a.max_ram as u64).replace(' ', ""))); }
    if a.workers_given { flags.push(format!("-n {}", a.num_workers)); }
    if !flags.is_empty() {
        s.push_str(&format!(" ({})", flags.join(" ")));
    }
    s
}

/// The input's file name, as it appears in log names and `log <name>`.
pub fn input_name(a: &Args) -> String {
    a.input_dir.as_deref()
        .and_then(Path::file_name)
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "job".into())
}

pub fn new_log_path(dir: &Path, id: JobId, a: &Args, at: SystemTime) -> PathBuf {
    dir.join(format!("{}_{}_{}.log", format_filename_time(at), id, input_name(a)))
}

/// (id, input name) from a log file name written by `new_log_path`.
fn parse_log_name(name: &str) -> Option<(JobId, &str)> {
    // the datetime is fixed width: YYYY-MM-DD_HH-MM-SS
    let rest = name.get(20..)?.strip_suffix(".log")?;
    if name.as_bytes().get(19) != Some(&b'_') {
        return None;
    }
    let (id, input) = rest.split_once('_')?;
    Some((id.parse().ok()?, input))
}

/// Logs in `dir`, oldest first (names start with the datetime).
pub fn list_logs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.file_name().and_then(|n| n.to_str()).and_then(parse_log_name).is_some())
        .collect();
    v.sort();
    v
}

/// Logs for a job id, or for an input name (with or without its extension), oldest first.
pub fn find_logs(dir: &Path, key: &str) -> Vec<PathBuf> {
    let id: Option<JobId> = key.parse().ok();
    let key_stem = Path::new(key).file_stem().and_then(|s| s.to_str()).unwrap_or(key);
    list_logs(dir).into_iter().filter(|p| {
        let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let Some((log_id, input)) = parse_log_name(name) else { return false };
        match id {
            Some(id) => log_id == id,
            None => input == key || Path::new(input).file_stem().and_then(|s| s.to_str()) == Some(key_stem),
        }
    }).collect()
}

pub fn max_logged_id(dir: &Path) -> JobId {
    list_logs(dir).iter()
        .filter_map(|p| parse_log_name(p.file_name()?.to_str()?).map(|(id, _)| id))
        .max()
        .unwrap_or(0)
}

/// Deletes the oldest logs beyond `keep`, never touching `protect` (logs still being written).
pub fn prune_logs(dir: &Path, keep: usize, protect: &[&Path]) {
    let logs = list_logs(dir);
    let excess = logs.len().saturating_sub(keep);
    for p in logs.iter().filter(|p| !protect.contains(&p.as_path())).take(excess) {
        let _ = fs::remove_file(p);
    }
}

// Queue persistence: the queue is saved on every change, so it survives a stop or crash.
// Format: per job, u64 id, u64 submitted (unix secs), u32 argc, then each arg as
// u32 length + path bytes.

pub fn queue_path() -> PathBuf {
    server_dir().join("queue")
}

pub fn save_queue<'a, I: IntoIterator<Item = &'a Job>>(jobs: I) -> io::Result<()> {
    let mut b = Vec::new();
    for j in jobs {
        let argv = j.args.to_argv();
        b.extend_from_slice(&j.id.to_le_bytes());
        let secs = j.submitted.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        b.extend_from_slice(&secs.to_le_bytes());
        b.extend_from_slice(&(argv.len() as u32).to_le_bytes());
        for a in argv {
            let bytes = path_to_bytes(Path::new(&a))?;
            b.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            b.extend_from_slice(&bytes);
        }
    }
    let path = queue_path();
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, b)?;
    fs::rename(tmp, path)
}

/// Saved jobs, with any whose arguments no longer check out (e.g. a path was deleted)
/// returned separately with the reason.
pub fn load_queue() -> (Vec<Job>, Vec<(JobId, String)>) {
    let Ok(b) = fs::read(queue_path()) else { return (Vec::new(), Vec::new()) };
    let mut r = b.as_slice();
    let mut jobs = Vec::new();
    let mut bad = Vec::new();
    while !r.is_empty() {
        let Some((id, secs, argv)) = read_job(&mut r) else {
            bad.push((0, "queue file is corrupt; the rest of it was skipped".to_string()));
            break;
        };
        match parse_job(argv) {
            Ok(args) => jobs.push(Job { id, args, submitted: UNIX_EPOCH + Duration::from_secs(secs) }),
            Err(e) => bad.push((id, e)),
        }
    }
    (jobs, bad)
}

fn read_job(r: &mut &[u8]) -> Option<(JobId, u64, Vec<OsString>)> {
    fn take<'a>(r: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if r.len() < n { return None; }
        let (a, b) = r.split_at(n);
        *r = b;
        Some(a)
    }
    let id = u64::from_le_bytes(take(r, 8)?.try_into().ok()?);
    let secs = u64::from_le_bytes(take(r, 8)?.try_into().ok()?);
    let argc = u32::from_le_bytes(take(r, 4)?.try_into().ok()?) as usize;
    let mut argv = Vec::with_capacity(argc.min(64));
    for _ in 0..argc {
        let len = u32::from_le_bytes(take(r, 4)?.try_into().ok()?) as usize;
        argv.push(bytes_to_path(take(r, len)?.to_vec()).ok()?.into_os_string());
    }
    Some((id, secs, argv))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_names() {
        assert_eq!(parse_log_name("2026-11-24_19-18-05_12_my_photos.log"), Some((12, "my_photos")));
        assert_eq!(parse_log_name("2026-11-24_19-18-05_x_a.log"), None);
        assert_eq!(parse_log_name("server.log"), None);
    }

    #[test]
    fn find_and_prune() {
        let dir = std::env::temp_dir().join(format!("tard_test_{}_logs", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        for (t, id, name) in [("01", 1, "photos"), ("02", 2, "photos.tard"), ("03", 3, "music")] {
            fs::write(dir.join(format!("2026-11-24_19-18-{}_{}_{}.log", t, id, name)), "").unwrap();
        }
        assert_eq!(find_logs(&dir, "2").len(), 1);
        assert_eq!(find_logs(&dir, "photos").len(), 2);
        assert_eq!(find_logs(&dir, "photos.tard").len(), 2);
        assert_eq!(max_logged_id(&dir), 3);
        let oldest = list_logs(&dir)[0].clone();
        prune_logs(&dir, 1, &[&oldest]);
        let left = list_logs(&dir);
        assert_eq!(left, vec![oldest]); // protected logs count toward `keep` but survive

        let _ = fs::remove_dir_all(&dir);
    }
}
