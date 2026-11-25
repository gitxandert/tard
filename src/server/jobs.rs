// Jobs as the server sees them, and their log files:
// ~/.tard/server/logs/<YYYY-MM-DD_HH-MM-SS>_<id>_<input name>.log (UTC)
use std::{
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use crate::cli::{Args, Cmd, parse_args_from};
use crate::server::queue::{ChainId, Lane};
use crate::server::server_dir;
use crate::utils::formatting::{format_size, parse_size};
use crate::utils::logger::format_filename_time;
use crate::utils::path::{bytes_to_path, path_to_bytes};

pub type JobId = u64;

#[derive(Clone)]
pub struct Job {
    pub id: JobId,
    pub args: Args,
    pub submitted: SystemTime,
    /// the schedule that started this run, if any
    pub schedule: Option<String>,
}

pub fn logs_dir() -> PathBuf {
    server_dir().join("logs")
}

/// logs/ for submitted jobs, logs/scheduled/<name>/ for scheduled runs.
pub fn log_dir_for(job: &Job) -> PathBuf {
    match &job.schedule {
        Some(name) => scheduled_log_dir(name),
        None => logs_dir(),
    }
}

pub fn scheduled_log_dir(name: &str) -> PathBuf {
    logs_dir().join("scheduled").join(name)
}

/// logs/ and every logs/scheduled/<name>/.
fn all_log_dirs() -> Vec<PathBuf> {
    let mut v = vec![logs_dir()];
    if let Ok(rd) = fs::read_dir(logs_dir().join("scheduled")) {
        v.extend(rd.flatten().map(|e| e.path()).filter(|p| p.is_dir()));
    }
    v
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
    if let Some(l) = &a.file_log { flags.push(format!("-l {}", l.display())); }
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

/// Logs for a job id, or for an input name (with or without its extension), oldest
/// first, from every log directory.
pub fn find_logs(key: &str) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = all_log_dirs().iter().flat_map(|d| find_logs_in(d, key)).collect();
    v.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    v
}

fn find_logs_in(dir: &Path, key: &str) -> Vec<PathBuf> {
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

pub fn max_logged_id() -> JobId {
    all_log_dirs().iter().flat_map(|d| list_logs(d))
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

// Queue persistence: saved on every change, so the queue survives a stop or crash.
// Format: MAGIC, then per job: u64 id, u64 submitted (unix secs), u64 chain, u32 group,
// u8 lane (0 main, 1 express, 2 paused), schedule name (u32 length + bytes, empty for
// none), u32 argc, then each arg as u32 length + bytes.

const MAGIC: &[u8] = b"tard-queue-2\n";

pub fn queue_path() -> PathBuf {
    server_dir().join("queue")
}

pub fn save_queue(entries: &[(&Job, ChainId, usize, Option<Lane>)]) -> io::Result<()> {
    let mut b = MAGIC.to_vec();
    for (j, chain, group, lane) in entries {
        b.extend_from_slice(&j.id.to_le_bytes());
        let secs = j.submitted.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        b.extend_from_slice(&secs.to_le_bytes());
        b.extend_from_slice(&chain.to_le_bytes());
        b.extend_from_slice(&(*group as u32).to_le_bytes());
        b.push(match lane { Some(Lane::Main) => 0, Some(Lane::Express) => 1, None => 2 });
        let sched = j.schedule.as_deref().unwrap_or("").as_bytes();
        b.extend_from_slice(&(sched.len() as u32).to_le_bytes());
        b.extend_from_slice(sched);
        let argv = j.args.to_argv();
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
pub fn load_queue() -> (Vec<(Job, ChainId, usize, Option<Lane>)>, Vec<(JobId, String)>) {
    let Ok(b) = fs::read(queue_path()) else { return (Vec::new(), Vec::new()) };
    let mut jobs = Vec::new();
    let mut bad = Vec::new();
    let Some(mut r) = b.strip_prefix(MAGIC) else {
        bad.push((0, "queue file is from another version; ignored".to_string()));
        return (jobs, bad);
    };
    while !r.is_empty() {
        let Some((id, secs, chain, group, lane, schedule, argv)) = read_job(&mut r) else {
            bad.push((0, "queue file is corrupt; the rest of it was skipped".to_string()));
            break;
        };
        match parse_job(argv) {
            Ok(args) => {
                let job = Job { id, args, submitted: UNIX_EPOCH + Duration::from_secs(secs), schedule };
                jobs.push((job, chain, group, lane));
            }
            Err(e) => bad.push((id, e)),
        }
    }
    (jobs, bad)
}

type Entry = (JobId, u64, ChainId, usize, Option<Lane>, Option<String>, Vec<OsString>);

fn read_job(r: &mut &[u8]) -> Option<Entry> {
    fn take<'a>(r: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
        if r.len() < n { return None; }
        let (a, b) = r.split_at(n);
        *r = b;
        Some(a)
    }
    let u64_ = |r: &mut &[u8]| take(r, 8).map(|b| u64::from_le_bytes(b.try_into().unwrap()));
    let u32_ = |r: &mut &[u8]| take(r, 4).map(|b| u32::from_le_bytes(b.try_into().unwrap()));
    let id = u64_(r)?;
    let secs = u64_(r)?;
    let chain = u64_(r)?;
    let group = u32_(r)? as usize;
    let lane = match take(r, 1)?[0] { 0 => Some(Lane::Main), 1 => Some(Lane::Express), 2 => None, _ => return None };
    let slen = u32_(r)? as usize;
    let sched = String::from_utf8(take(r, slen)?.to_vec()).ok()?;
    let schedule = (!sched.is_empty()).then_some(sched);
    let argc = u32_(r)? as usize;
    let mut argv = Vec::with_capacity(argc.min(64));
    for _ in 0..argc {
        let len = u32_(r)? as usize;
        argv.push(bytes_to_path(take(r, len)?.to_vec()).ok()?.into_os_string());
    }
    Some((id, secs, chain, group, lane, schedule, argv))
}

// Chains and edits on the command line

fn is_sep(a: &OsStr) -> Option<Sep> {
    match a.to_str()? {
        "-q" | "--queue" => Some(Sep::Queue),
        "-p" | "--parallel" => Some(Sep::Parallel),
        _ => None,
    }
}

#[derive(PartialEq)]
enum Sep {
    Queue,
    Parallel,
}

/// Splits `A.. -p B.. -q C..` into groups of jobs' argv: [[A, B], [C]]. A leading -p puts
/// the whole submission in the express lane; a leading -q is the same as none.
pub fn split_chain(tokens: Vec<OsString>) -> Result<(Lane, Vec<Vec<Vec<OsString>>>), String> {
    let mut lane = Lane::Main;
    let mut groups: Vec<Vec<Vec<OsString>>> = vec![vec![Vec::new()]];
    for (i, t) in tokens.into_iter().enumerate() {
        let Some(sep) = is_sep(&t) else {
            groups.last_mut().unwrap().last_mut().unwrap().push(t);
            continue;
        };
        if i == 0 {
            if sep == Sep::Parallel { lane = Lane::Express; }
            continue;
        }
        if groups.last().unwrap().last().unwrap().is_empty() {
            return Err(format!("{} must follow a job's arguments", t.to_string_lossy()));
        }
        match sep {
            Sep::Parallel => groups.last_mut().unwrap().push(Vec::new()),
            Sep::Queue => groups.push(vec![Vec::new()]),
        }
    }
    if groups.last().unwrap().last().unwrap().is_empty() {
        return Err("expected a job (<input> <output> [options]) after the last -q/-p".into());
    }
    Ok((lane, groups))
}

/// Rebuilds a chain as tokens for `split_chain` on the server.
pub fn join_chain(lane: Lane, groups: &[Vec<Args>]) -> Vec<OsString> {
    let mut v: Vec<OsString> = Vec::new();
    if lane == Lane::Express { v.push("-p".into()); }
    for (gi, g) in groups.iter().enumerate() {
        for (ji, a) in g.iter().enumerate() {
            if ji > 0 { v.push("-p".into()); } else if gi > 0 { v.push("-q".into()); }
            v.extend(a.to_argv());
        }
    }
    v
}

/// Makes the paths in `edit` tokens absolute (they're resolved on the server otherwise).
pub fn absolutize(tokens: Vec<OsString>) -> Result<Vec<OsString>, String> {
    let mut out = Vec::with_capacity(tokens.len());
    let mut it = tokens.into_iter();
    while let Some(t) = it.next() {
        let s = t.to_string_lossy();
        if s == "-r" || s == "--ram" || s == "-n" || s == "--num_workers" {
            out.push(t);
            out.extend(it.next());
        } else if s == "-l" || s == "--log" {
            out.push(t);
            match it.next() {
                Some(v) if v == "default" => out.push(v), // edit: back to the config's directory
                // may not exist yet (a file path), so absolute rather than canonical
                Some(v) => out.push(std::path::absolute(&v).map_err(|e| format!("{}: {}", v.to_string_lossy(), e))?.into_os_string()),
                None => (),
            }
        } else if s.starts_with('-') {
            out.push(t);
        } else {
            let p = PathBuf::from(&t).canonicalize().map_err(|e| format!("{}: {}", s, e))?;
            out.push(p.into_os_string());
        }
    }
    Ok(out)
}

/// `edit <id> <tokens>`: options given override the job's, `-r auto` / `-n auto` hand the
/// choice back to the server, `-l default` goes back to the config's log directory, and
/// paths replace the input (then the output).
pub fn edit_args(old: &Args, tokens: &[OsString]) -> Result<Args, String> {
    let mut a = old.clone();
    let mut paths = 0;
    let mut it = tokens.iter();
    while let Some(t) = it.next() {
        match t.to_str().unwrap_or("") {
            "-a" | "--archive" => a.cmd = Cmd::Archive,
            "-x" | "--extract" => a.cmd = Cmd::Extract,
            "-c" | "--continue" => a.resume = true,
            "-v" | "--verbose" => a.verbose = true,
            "-l" | "--log" => {
                let v = it.next().ok_or("unspecified value for -l/--log")?;
                a.file_log = if v == "default" { None } else { Some(std::path::absolute(v).map_err(|e| e.to_string())?) };
            }
            f @ ("-r" | "--ram" | "-n" | "--num_workers") => {
                let v = it.next().ok_or(format!("unspecified value for {}", f))?.to_string_lossy();
                let ram = f == "-r" || f == "--ram";
                if v == "auto" {
                    if ram { a.ram_given = false } else { a.workers_given = false }
                } else if ram {
                    a.max_ram = parse_size(&v)?;
                    a.ram_given = true;
                } else {
                    a.num_workers = v.parse().ok().filter(|n| *n > 0).ok_or("-n needs a number of at least 1")?;
                    a.workers_given = true;
                }
            }
            s if s.starts_with('-') => return Err(format!("unknown option {}", s)),
            _ => {
                let p = PathBuf::from(t).canonicalize().map_err(|e| format!("{}: {}", t.to_string_lossy(), e))?;
                match paths {
                    0 => a.input_dir = Some(p),
                    1 => a.output_dir = Some(p),
                    _ => return Err("an edit takes at most an input and an output".into()),
                }
                paths += 1;
            }
        }
    }
    validate(&a)?;
    Ok(a)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn os(v: &[&str]) -> Vec<OsString> {
        v.iter().map(OsString::from).collect()
    }

    #[test]
    fn chains() {
        let (lane, g) = split_chain(os(&["a", "b", "-p", "c", "d", "-q", "e", "f"])).unwrap();
        assert_eq!(lane, Lane::Main);
        assert_eq!(g, vec![vec![os(&["a", "b"]), os(&["c", "d"])], vec![os(&["e", "f"])]]);
        let (lane, g) = split_chain(os(&["-p", "a", "b"])).unwrap();
        assert_eq!((lane, g.len()), (Lane::Express, 1));
        assert_eq!(split_chain(os(&["-q", "a", "b"])).unwrap().0, Lane::Main);
        assert!(split_chain(os(&["a", "b", "-q"])).is_err());
        assert!(split_chain(os(&["a", "b", "-q", "-p", "c", "d"])).is_err());
    }

    #[test]
    fn edits() {
        let old = parse_job(os(&["/", "/", "-r", "64MB"])).unwrap();
        let e = edit_args(&old, &os(&["-r", "auto", "-n", "3", "-v"])).unwrap();
        assert!(!e.ram_given && e.workers_given && e.num_workers == 3 && e.verbose);
        assert!(edit_args(&old, &os(&["-z"])).is_err());
        assert!(edit_args(&old, &os(&["/nonexistent-path"])).is_err());
    }

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
        assert_eq!(find_logs_in(&dir, "2").len(), 1);
        assert_eq!(find_logs_in(&dir, "photos").len(), 2);
        assert_eq!(find_logs_in(&dir, "photos.tard").len(), 2);
        let oldest = list_logs(&dir)[0].clone();
        prune_logs(&dir, 1, &[&oldest]);
        let left = list_logs(&dir);
        assert_eq!(left, vec![oldest]); // protected logs count toward `keep` but survive

        let _ = fs::remove_dir_all(&dir);
    }
}
