// Scheduled jobs, kept in ~/.tard/server/schedule.toml. Times are local (the system
// time zone) and stored as RFC 9557 strings, e.g. 2026-11-26T03:00:00-05:00[America/New_York].
use std::{fs, path::PathBuf};
use jiff::{Span, ZonedDateTime, civil::DateTime, tz::TimeZone};
use serde::{Deserialize, Serialize};
use crate::server::server_dir;

pub type Zdt = ZonedDateTime;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Schedule {
    pub name: String,
    /// the job's arguments, as `tard server` would get them (absolute paths)
    pub args: Vec<String>,
    /// "once", "every 6h", "mon,thu 03:00", ...
    pub repeat: String,
    /// total runs; none = a series that repeats forever
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runs: Option<u32>,
    #[serde(default)]
    pub done: u32,
    /// none once the schedule has run out
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    #[serde(default)]
    pub history: Vec<Run>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Run {
    pub at: String,
    pub job: u64,
    pub status: String,
    #[serde(default)]
    pub secs: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct ScheduleFile {
    #[serde(default, rename = "schedule")]
    schedules: Vec<Schedule>,
}

pub fn schedule_path() -> PathBuf {
    server_dir().join("schedule.toml")
}

pub fn load() -> Result<Vec<Schedule>, String> {
    let path = schedule_path();
    match fs::read_to_string(&path) {
        Ok(t) => toml::from_str::<ScheduleFile>(&t)
            .map(|f| f.schedules)
            .map_err(|e| format!("{}: {}", path.display(), e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("could not read {}: {}", path.display(), e)),
    }
}

pub fn save(schedules: &[Schedule]) -> Result<(), String> {
    let path = schedule_path();
    let text = toml::to_string(&ScheduleFile { schedules: schedules.to_vec() }).map_err(|e| e.to_string())?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, text).and_then(|_| fs::rename(&tmp, &path))
        .map_err(|e| format!("could not write {}: {}", path.display(), e))
}

/// Names tell schedules apart from job ids, so they can't be all digits.
pub fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("a schedule name needs 1-64 characters".into());
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')) || name.starts_with(['-', '.']) {
        return Err(format!("'{}': use letters, digits, '-', '_' and '.' (not first)", name));
    }
    if name.chars().all(|c| c.is_ascii_digit()) {
        return Err("a schedule name can't be just digits (those are job ids)".into());
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq)]
pub enum Repeat {
    Once,
    /// every n minutes/hours/days/weeks ('m', 'h', 'd', 'w'); days and weeks keep the
    /// local time of day across DST changes
    Every(i64, char),
    /// on these weekdays (Monday = 0) at hour:minute
    Weekly([bool; 7], i8, i8),
}

const DAYS: [&str; 7] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];

pub fn parse_repeat(s: &str) -> Result<Repeat, String> {
    let s = s.trim().to_ascii_lowercase();
    let help = "use once, every <N>m/h/d/w (e.g. every 6h), or days and a time (e.g. mon,thu 03:00, daily 03:00, weekdays 09:30)";
    if s == "once" {
        return Ok(Repeat::Once);
    }
    if let Some(rest) = s.strip_prefix("every") {
        let rest: String = rest.chars().filter(|c| !c.is_whitespace()).collect();
        let unit = rest.chars().last().ok_or(help)?;
        let n: i64 = rest[..rest.len() - 1].parse().map_err(|_| help)?;
        if !matches!(unit, 'm' | 'h' | 'd' | 'w') || n < 1 || n > 10_000 {
            return Err(help.into());
        }
        return Ok(Repeat::Every(n, unit));
    }
    let (days_s, time_s) = s.split_once(char::is_whitespace).ok_or(help)?;
    let mut days = [false; 7];
    match days_s {
        "daily" => days = [true; 7],
        "weekdays" => days[..5].fill(true),
        "weekends" => days[5..].fill(true),
        list => {
            for d in list.split(',') {
                let i = DAYS.iter().position(|n| d.trim().starts_with(n) && d.trim().len() >= 3).ok_or(help)?;
                days[i] = true;
            }
        }
    }
    let (h, m) = parse_hm(time_s.trim()).ok_or(help)?;
    Ok(Repeat::Weekly(days, h, m))
}

fn parse_hm(s: &str) -> Option<(i8, i8)> {
    let (h, m) = s.split_once(':')?;
    let (h, m): (i8, i8) = (h.parse().ok()?, m.parse().ok()?);
    ((0..24).contains(&h) && (0..60).contains(&m)).then_some((h, m))
}

impl Repeat {
    /// The canonical spelling, as stored.
    pub fn to_text(&self) -> String {
        match self {
            Repeat::Once => "once".into(),
            Repeat::Every(n, u) => format!("every {}{}", n, u),
            Repeat::Weekly(days, h, m) => {
                let d = if days.iter().all(|d| *d) {
                    "daily".to_string()
                } else if days[..5].iter().all(|d| *d) && !days[5] && !days[6] {
                    "weekdays".to_string()
                } else if days[5] && days[6] && !days[..5].iter().any(|d| *d) {
                    "weekends".to_string()
                } else {
                    DAYS.iter().zip(days).filter(|(_, on)| **on).map(|(n, _)| *n).collect::<Vec<_>>().join(",")
                };
                format!("{} {:02}:{:02}", d, h, m)
            }
        }
    }

    /// The first run at or after `start`.
    pub fn first(&self, start: &Zdt) -> Option<Zdt> {
        match self {
            Repeat::Weekly(..) => self.next_slot(&start.checked_sub(Span::new().seconds(1)).ok()?),
            _ => Some(start.clone()),
        }
    }

    /// The first run after `after`, counting from `prev` (the last scheduled run time)
    /// so `every` intervals stay on their grid. Runs missed while the server was down
    /// are skipped, not made up one by one.
    pub fn next(&self, prev: &Zdt, after: &Zdt) -> Option<Zdt> {
        match self {
            Repeat::Once => None,
            Repeat::Every(n, u) => {
                let step = match u {
                    'm' => Span::new().minutes(*n),
                    'h' => Span::new().hours(*n),
                    'd' => Span::new().days(*n),
                    _ => Span::new().weeks(*n),
                };
                let mut t = prev.checked_add(step).ok()?;
                // jump close first, so a long-down server with `every 1m` doesn't loop for ages
                let approx = match u { 'm' => 60, 'h' => 3600, 'd' => 86_400, _ => 604_800 } * n;
                let behind = after.timestamp().as_second() - t.timestamp().as_second();
                if behind > approx * 2 {
                    let k = behind / approx - 1;
                    t = t.checked_add(span_times(step, k)?).ok()?;
                }
                while t <= *after {
                    t = t.checked_add(step).ok()?;
                }
                Some(t)
            }
            Repeat::Weekly(..) => self.next_slot(after),
        }
    }

    fn next_slot(&self, after: &Zdt) -> Option<Zdt> {
        let Repeat::Weekly(days, h, m) = self else { return None };
        let tz = after.time_zone().clone();
        for d in 0..=7 {
            let date = after.date().checked_add(Span::new().days(d)).ok()?;
            if !days[date.weekday().to_monday_zero_offset() as usize] {
                continue;
            }
            let t = date.at(*h, *m, 0, 0).to_zoned_datetime(tz.clone()).ok()?;
            if t > *after {
                return Some(t);
            }
        }
        None
    }
}

fn span_times(step: Span, k: i64) -> Option<Span> {
    let s = Span::new();
    Some(if step.get_minutes() != 0 {
        s.minutes(step.get_minutes().checked_mul(k)?)
    } else if step.get_hours() != 0 {
        s.hours(i64::from(step.get_hours()).checked_mul(k)?)
    } else if step.get_days() != 0 {
        s.days(i64::from(step.get_days()).checked_mul(k)?)
    } else {
        s.weeks(i64::from(step.get_weeks()).checked_mul(k)?)
    })
}

/// "now", "HH:MM" (the next time it's that time), or "YYYY-MM-DD HH:MM", in local time.
pub fn parse_when(s: &str) -> Result<Zdt, String> {
    let s = s.trim();
    let now = Zdt::now().round(jiff::Unit::Second).map_err(|e| e.to_string())?;
    if s.is_empty() || s.eq_ignore_ascii_case("now") {
        return Ok(now);
    }
    let help = || format!("'{}': use now, HH:MM, or YYYY-MM-DD HH:MM", s);
    if let Some((h, m)) = parse_hm(s) {
        let today = now.date().at(h, m, 0, 0).to_zoned_datetime(TimeZone::system()).map_err(|e| e.to_string())?;
        return if today > now { Ok(today) } else { today.checked_add(Span::new().days(1)).map_err(|e| e.to_string()) };
    }
    let dt: DateTime = s.replacen(' ', "T", 1).parse().map_err(|_| help())?;
    dt.to_zoned_datetime(TimeZone::system()).map_err(|e| e.to_string())
}

pub fn parse_zdt(s: &str) -> Option<Zdt> {
    s.parse().ok()
}

/// e.g. "2026-11-26 03:00"
pub fn show(t: &Zdt) -> String {
    t.strftime("%Y-%m-%d %H:%M").to_string()
}

impl Schedule {
    pub fn next_time(&self) -> Option<Zdt> {
        self.next.as_deref().and_then(parse_zdt)
    }

    /// "once", "every 1d, forever", "every 6h, 3 of 5 runs done"
    pub fn regularity(&self) -> String {
        match self.runs {
            _ if self.repeat == "once" => "once".into(),
            None => format!("{}, forever", self.repeat),
            Some(n) => format!("{}, {} of {} runs done", self.repeat, self.done, n),
        }
    }

    /// Counts a run that was due at `due` and works out the next one after `now`.
    pub fn advance(&mut self, due: &Zdt, now: &Zdt) {
        self.done += 1;
        let repeat = parse_repeat(&self.repeat).unwrap_or(Repeat::Once);
        let used_up = self.runs.is_some_and(|n| self.done >= n);
        self.next = if used_up { None } else { repeat.next(due, now).map(|t| t.to_string()) };
    }

    pub fn record(&mut self, run: Run, keep: usize) {
        self.history.push(run);
        let excess = self.history.len().saturating_sub(keep.max(1));
        self.history.drain(..excess);
    }
}

// Shell-style quoting for job arguments typed at the prompts.

/// Splits like a shell: whitespace separates words, '...' and "..." group, \ escapes.
pub fn shell_split(s: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut cur = String::new();
    let mut in_word = false;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('\'') => break,
                        Some(c) => cur.push(c),
                        None => return Err("unclosed '".into()),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next() {
                        Some('"') => break,
                        Some('\\') => cur.push(chars.next().ok_or("trailing \\")?),
                        Some(c) => cur.push(c),
                        None => return Err("unclosed \"".into()),
                    }
                }
            }
            '\\' => {
                in_word = true;
                cur.push(chars.next().ok_or("trailing \\")?);
            }
            c if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut cur));
                    in_word = false;
                }
            }
            c => {
                in_word = true;
                cur.push(c);
            }
        }
    }
    if in_word {
        words.push(cur);
    }
    Ok(words)
}

/// The inverse of shell_split: one line, quoted only where needed.
pub fn shell_join(words: &[String]) -> String {
    words.iter().map(|w| {
        if !w.is_empty() && w.chars().all(|c| c.is_ascii_alphanumeric() || "-_./:=@,+%".contains(c)) {
            w.clone()
        } else {
            format!("'{}'", w.replace('\'', "'\\''"))
        }
    }).collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> Zdt {
        format!("{}[UTC]", s).parse().unwrap()
    }

    #[test]
    fn repeats_parse_and_print() {
        for (input, canon) in [
            ("once", "once"),
            ("every 6h", "every 6h"),
            ("Every 2 D", "every 2d"),
            ("mon,thu 03:00", "mon,thu 03:00"),
            ("monday,friday 9:05", "mon,fri 09:05"),
            ("daily 23:59", "daily 23:59"),
            ("weekdays 09:30", "weekdays 09:30"),
        ] {
            assert_eq!(parse_repeat(input).unwrap().to_text(), canon, "{}", input);
        }
        for bad in ["every", "every 0h", "every 5y", "fri", "mon 25:00", "xyz 03:00"] {
            assert!(parse_repeat(bad).is_err(), "{}", bad);
        }
    }

    #[test]
    fn every_skips_missed_runs_and_keeps_grid() {
        let r = parse_repeat("every 6h").unwrap();
        let prev = at("2026-11-01T00:00:00Z");
        assert_eq!(r.next(&prev, &prev).unwrap(), at("2026-11-01T06:00:00Z"));
        // server was down for 3 days: next slot after now, on the 6h grid
        let now = at("2026-11-04T07:30:00Z");
        assert_eq!(r.next(&prev, &now).unwrap(), at("2026-11-04T12:00:00Z"));
        let m = parse_repeat("every 1m").unwrap();
        assert_eq!(m.next(&prev, &at("2027-11-01T00:00:30Z")).unwrap(), at("2027-11-01T00:01:00Z"));
    }

    #[test]
    fn weekly_slots() {
        let r = parse_repeat("mon,thu 03:00").unwrap();
        // 2026-11-24 is a Tuesday
        let tue = at("2026-11-24T10:00:00Z");
        assert_eq!(r.first(&tue).unwrap(), at("2026-11-26T03:00:00Z"));
        let thu = at("2026-11-26T03:00:00Z");
        assert_eq!(r.first(&thu).unwrap(), thu); // exactly on a slot counts
        assert_eq!(r.next(&thu, &thu).unwrap(), at("2026-11-30T03:00:00Z"));
    }

    #[test]
    fn advance_counts_runs() {
        let mut s = Schedule {
            name: "n".into(), args: vec![], repeat: "every 1d".into(), runs: Some(2),
            done: 0, next: None, history: vec![],
        };
        let t = at("2026-11-24T03:00:00Z");
        s.advance(&t, &t);
        assert_eq!(s.next_time().unwrap(), at("2026-11-25T03:00:00Z"));
        s.advance(&t, &t);
        assert_eq!(s.next, None);
        for i in 0..5 {
            s.record(Run { at: String::new(), job: i, status: "ok".into(), secs: 0 }, 3);
        }
        assert_eq!(s.history.iter().map(|r| r.job).collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn file_round_trip() {
        let s = Schedule {
            name: "photos".into(), args: vec!["-a".into(), "/a b".into()], repeat: "once".into(),
            runs: Some(1), done: 0, next: Some(at("2026-11-24T03:00:00Z").to_string()), history: vec![],
        };
        let text = toml::to_string(&ScheduleFile { schedules: vec![s.clone()] }).unwrap();
        assert_eq!(toml::from_str::<ScheduleFile>(&text).unwrap().schedules, vec![s]);
    }

    #[test]
    fn names_and_quoting() {
        assert!(check_name("photos-nightly").is_ok());
        assert!(check_name("123").is_err());
        assert!(check_name("-x").is_err());
        assert!(check_name("a b").is_err());
        let words = vec!["-a".to_string(), "/my photos".to_string(), "it's".to_string()];
        assert_eq!(shell_split(&shell_join(&words)).unwrap(), words);
        assert_eq!(shell_split(r#"a "b c" d\ e"#).unwrap(), vec!["a", "b c", "d e"]);
        assert!(shell_split("'open").is_err());
    }
}
