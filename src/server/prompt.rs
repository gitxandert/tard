// Interactive prompts for `tard server schedule new/edit`. They run in the client (where
// relative paths mean something) and produce the server's schedule-add/-set request.
use std::{
    ffi::OsString,
    io::{self, BufRead, Write},
};
use crate::config::expand_tilde; // the shell isn't there to expand ~ in a typed line
use crate::server::jobs;
use crate::server::schedule::{self, Repeat, Zdt};

/// Current values, as `schedule-get` reports them, used as defaults when editing.
pub struct Current {
    pub repeat: String,
    pub runs: String,
    pub next: String,
    pub args: String,
}

impl Current {
    pub fn parse(text: &str) -> Option<Self> {
        let mut l = text.lines();
        Some(Self {
            repeat: l.next()?.to_string(),
            runs: l.next()?.to_string(),
            next: l.next()?.to_string(),
            args: l.next()?.to_string(),
        })
    }
}

/// Walks through job → regularity → date-time, then confirms. Returns the request, or
/// None if the user said no.
pub fn schedule_request(name: &str, current: Option<&Current>) -> Result<Option<Vec<OsString>>, String> {
    let editing = current.is_some();
    println!("{} scheduled job '{}' (Ctrl-D to cancel)", if editing { "Editing" } else { "New" }, name);

    // 1. the job
    let args: Vec<String> = loop {
        let line = ask("Job arguments, as after `tard server` (e.g. -a ~/photos /backup -r 64MB)", current.map(|c| c.args.as_str()))?;
        match check_job(&line) {
            Ok(a) => break a,
            Err(e) => println!("  {}", e),
        }
    };

    // 2. how often
    let (repeat, runs) = loop {
        let default = current.map(|c| match (c.repeat.as_str(), c.runs.as_str()) {
            ("once", _) => "once".to_string(),
            (_, "forever") => "series".to_string(),
            (_, n) => n.to_string(),
        });
        let how = ask("Runs: once, a number of runs (e.g. 5), or series (forever)", default.as_deref().or(Some("once")))?;
        let runs = match how.to_ascii_lowercase().as_str() {
            "once" | "1" => break (Repeat::Once, "1".to_string()),
            "series" | "forever" => "forever".to_string(),
            n => match n.parse::<u32>() {
                Ok(n) if n > 1 => n.to_string(),
                _ => { println!("  type once, a number like 5, or series"); continue; }
            },
        };
        let repeat = loop {
            let default = current.map(|c| c.repeat.as_str()).filter(|r| *r != "once");
            let r = ask("Repeat: every <N>m/h/d/w, or days and a time (every 6h, every 1d, mon,thu 03:00, daily 03:00)", default)?;
            match schedule::parse_repeat(&r) {
                Ok(Repeat::Once) => println!("  that's a single run; give an interval or days and a time"),
                Ok(r) => break r,
                Err(e) => println!("  {}", e),
            }
        };
        break (repeat, runs);
    };

    // 3. when
    let first: Zdt = loop {
        let label = if editing { "Next run: now, HH:MM, or YYYY-MM-DD HH:MM" } else { "First run: now, HH:MM, or YYYY-MM-DD HH:MM" };
        let default = current.map(|c| c.next.as_str()).filter(|n| !n.is_empty()).unwrap_or("now");
        let when = ask(label, Some(default))?;
        let t = match schedule::parse_when(&when) {
            Ok(t) => t,
            Err(e) => { println!("  {}", e); continue; }
        };
        if t.timestamp().as_second() < Zdt::now().timestamp().as_second() - 60 {
            println!("  that's in the past");
            continue;
        }
        match repeat.first(&t) {
            Some(f) => break f,
            None => println!("  couldn't work out a run time from that"),
        }
    };

    let regularity = match (&repeat, runs.as_str()) {
        (Repeat::Once, _) => "once".to_string(),
        (r, "forever") => format!("{}, forever", r.to_text()),
        (r, n) => format!("{}, {} runs", r.to_text(), n),
    };
    println!();
    println!("  job:       {}", schedule::shell_join(&args));
    println!("  repeats:   {}", regularity);
    println!("  {}: {}", if editing { "next run " } else { "first run" }, schedule::show(&first));
    if !confirm("Save?")? {
        return Ok(None);
    }

    let mut req: Vec<OsString> = vec![
        if editing { "schedule-set" } else { "schedule-add" }.into(),
        name.into(),
        repeat.to_text().into(),
        runs.into(),
        first.to_string().into(),
    ];
    req.extend(args.into_iter().map(OsString::from));
    Ok(Some(req))
}

/// Parses and checks one job the way `tard server <job>` would, returning its normalized
/// arguments (absolute paths).
fn check_job(line: &str) -> Result<Vec<String>, String> {
    let words = schedule::shell_split(line)?;
    if words.is_empty() {
        return Err("give the job's input and output, plus any options".into());
    }
    let words: Vec<OsString> = words.into_iter().map(|w| expand_tilde(&w).into_os_string()).collect();
    if words.iter().any(|w| matches!(w.to_str(), Some("-q" | "--queue" | "-p" | "--parallel"))) {
        return Err("a scheduled job is a single job; -q/-p chains can't be scheduled".into());
    }
    let args = jobs::parse_job(words)?;
    args.to_argv().into_iter()
        .map(|a| a.into_string().map_err(|_| "paths that aren't valid UTF-8 can't be scheduled".to_string()))
        .collect()
}


fn ask(label: &str, default: Option<&str>) -> Result<String, String> {
    match default {
        Some(d) if !d.is_empty() => print!("{} [{}]: ", label, d),
        _ => print!("{}: ", label),
    }
    let _ = io::stdout().flush();
    let mut line = String::new();
    let n = io::stdin().lock().read_line(&mut line).map_err(|e| e.to_string())?;
    if n == 0 {
        println!();
        return Err("cancelled".into());
    }
    let line = line.trim();
    Ok(if line.is_empty() { default.unwrap_or("").to_string() } else { line.to_string() })
}

fn confirm(label: &str) -> Result<bool, String> {
    let a = ask(&format!("{} [Y/n]", label), None)?;
    Ok(!matches!(a.to_ascii_lowercase().as_str(), "n" | "no"))
}
