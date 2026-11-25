// ~/.tard/config.toml: server limits and log/schedule retention.
// A commented copy of the defaults is written the first time it's loaded.
use std::{
    fs, io,
    path::PathBuf,
    time::Duration,
};
use serde::{Deserialize, Deserializer, de::Error};
use crate::utils::formatting::{parse_duration, parse_size};

const TEMPLATE: &str = r#"# tard configuration. Sizes: 512KB, 64MB, 4GB. Durations: 30m, 12h, 2d, 1w.

[server]
# localhost port the server listens on
port = 8273
# RAM shared by all running jobs; "auto" = half of the memory available when the server starts
max_ram = "auto"
# worker threads shared by all running jobs; "auto" = number of CPUs
max_workers = "auto"
# jobs allowed to run at the same time
max_parallel_jobs = 4
# a job running longer than this is dropped; "none" = no limit
max_run_time = "none"

[jobs]
# bounds for jobs submitted without -r/-n, which get a fair share of what's free
min_ram = "10MB"
max_ram = "256MB"
min_workers = 1
max_workers = 8
# a job submitted with a leading -p (run now, alongside the queue) when there isn't room:
# "wait" for room ahead of the queue, or "shrink" to start now with what's free
express_when_full = "wait"

[logs]
# job logs kept in ~/.tard/server/logs (oldest are deleted first)
keep = 100

[schedule]
# past runs shown by `tard server schedule <name>`
history = 10
"#;

#[derive(Debug, PartialEq, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub server: Server,
    pub jobs: Jobs,
    pub logs: Logs,
    pub schedule: Schedule,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Server {
    pub port: u16,
    #[serde(deserialize_with = "size_limit")]
    pub max_ram: Limit,
    #[serde(deserialize_with = "count_limit")]
    pub max_workers: Limit,
    pub max_parallel_jobs: usize,
    #[serde(deserialize_with = "optional_duration")]
    pub max_run_time: Option<Duration>,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Jobs {
    #[serde(deserialize_with = "size")]
    pub min_ram: usize,
    #[serde(deserialize_with = "size")]
    pub max_ram: usize,
    pub min_workers: usize,
    pub max_workers: usize,
    pub express_when_full: WhenFull,
}

#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WhenFull {
    Wait,
    Shrink,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logs {
    pub keep: usize,
}

#[derive(Debug, PartialEq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Schedule {
    pub history: usize,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Limit {
    Auto,
    Fixed(usize),
}

impl Default for Server {
    fn default() -> Self {
        Self {
            port: 8273,
            max_ram: Limit::Auto,
            max_workers: Limit::Auto,
            max_parallel_jobs: 4,
            max_run_time: None,
        }
    }
}

impl Default for Jobs {
    fn default() -> Self {
        Self { min_ram: 10 << 20, max_ram: 256 << 20, min_workers: 1, max_workers: 8, express_when_full: WhenFull::Wait }
    }
}

impl Default for Logs {
    fn default() -> Self {
        Self { keep: 100 }
    }
}

impl Default for Schedule {
    fn default() -> Self {
        Self { history: 10 }
    }
}

impl Config {
    /// Reads `<tard dir>/config.toml`, writing the commented defaults first if it doesn't exist.
    pub fn load() -> Result<Self, String> {
        let path = config_path();
        let text = match fs::read_to_string(&path) {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                write_template(&path)
                    .map_err(|e| format!("could not create {}: {}", path.display(), e))?;
                TEMPLATE.to_string()
            }
            Err(e) => return Err(format!("could not read {}: {}", path.display(), e)),
        };
        Self::parse(&text).map_err(|e| format!("{}: {}", path.display(), e))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let c: Self = toml::from_str(text).map_err(|e| e.to_string())?;
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), String> {
        let j = &self.jobs;
        if self.server.max_parallel_jobs == 0 {
            return Err("server.max_parallel_jobs must be at least 1".into());
        }
        if self.server.max_workers == Limit::Fixed(0) || j.min_workers == 0 {
            return Err("worker counts must be at least 1".into());
        }
        if j.min_ram > j.max_ram {
            return Err("jobs.min_ram is larger than jobs.max_ram".into());
        }
        if j.min_workers > j.max_workers {
            return Err("jobs.min_workers is larger than jobs.max_workers".into());
        }
        Ok(())
    }

    /// RAM the server may hand out across all jobs, resolving "auto" now.
    pub fn server_ram(&self) -> usize {
        match self.server.max_ram {
            Limit::Fixed(n) => n,
            Limit::Auto => available_memory() / 2,
        }
    }

    /// Worker threads the server may hand out across all jobs, resolving "auto" now.
    pub fn server_workers(&self) -> usize {
        match self.server.max_workers {
            Limit::Fixed(n) => n,
            Limit::Auto => std::thread::available_parallelism().map_or(4, |n| n.get()),
        }
    }
}

/// `$TARD_HOME`, else `~/.tard`.
pub fn tard_dir() -> PathBuf {
    if let Some(d) = std::env::var_os("TARD_HOME") {
        return PathBuf::from(d);
    }
    std::env::home_dir().unwrap_or_else(|| PathBuf::from(".")).join(".tard")
}

pub fn config_path() -> PathBuf {
    tard_dir().join("config.toml")
}

fn write_template(path: &std::path::Path) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    fs::write(path, TEMPLATE)
}

/// MemAvailable from /proc/meminfo; 2GB where that isn't available.
fn available_memory() -> usize {
    const FALLBACK: usize = 2 << 30;
    let Ok(info) = fs::read_to_string("/proc/meminfo") else { return FALLBACK };
    info.lines()
        .find_map(|l| l.strip_prefix("MemAvailable:"))
        .and_then(|v| v.trim().strip_suffix("kB"))
        .and_then(|kb| kb.trim().parse::<usize>().ok())
        .map_or(FALLBACK, |kb| kb * 1024)
}

// field deserializers: sizes and counts may be written as numbers or strings

#[derive(Deserialize)]
#[serde(untagged)]
enum NumOrStr {
    Num(u64),
    Str(String),
}

fn size<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
    match NumOrStr::deserialize(d)? {
        NumOrStr::Num(n) => Ok(n as usize),
        NumOrStr::Str(s) => parse_size(&s).map_err(D::Error::custom),
    }
}

fn size_limit<'de, D: Deserializer<'de>>(d: D) -> Result<Limit, D::Error> {
    match NumOrStr::deserialize(d)? {
        NumOrStr::Str(s) if s == "auto" => Ok(Limit::Auto),
        NumOrStr::Str(s) => parse_size(&s).map(Limit::Fixed).map_err(D::Error::custom),
        NumOrStr::Num(n) => Ok(Limit::Fixed(n as usize)),
    }
}

fn count_limit<'de, D: Deserializer<'de>>(d: D) -> Result<Limit, D::Error> {
    match NumOrStr::deserialize(d)? {
        NumOrStr::Str(s) if s == "auto" => Ok(Limit::Auto),
        NumOrStr::Str(s) => Err(D::Error::custom(format!("expected a number or \"auto\", got '{}'", s))),
        NumOrStr::Num(n) => Ok(Limit::Fixed(n as usize)),
    }
}

fn optional_duration<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
    match NumOrStr::deserialize(d)? {
        NumOrStr::Str(s) if s == "none" => Ok(None),
        NumOrStr::Num(0) => Ok(None),
        NumOrStr::Str(s) => parse_duration(&s).map(Some).map_err(D::Error::custom),
        NumOrStr::Num(_) => Err(D::Error::custom("give a duration with a unit, e.g. \"12h\", or \"none\"")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn template_matches_defaults() {
        assert_eq!(Config::parse(TEMPLATE).unwrap(), Config::default());
    }

    #[test]
    fn partial_file_keeps_other_defaults() {
        let c = Config::parse("[server]\nmax_ram = \"8GB\"\nmax_run_time = \"12h\"\n").unwrap();
        assert_eq!(c.server.max_ram, Limit::Fixed(8 << 30));
        assert_eq!(c.server.max_run_time, Some(Duration::from_secs(12 * 3600)));
        assert_eq!(c.server.port, 8273);
        assert_eq!(c.logs.keep, 100);
    }

    #[test]
    fn rejects_bad_values() {
        assert!(Config::parse("[server]\nmax_ram = \"lots\"\n").is_err());
        assert!(Config::parse("[server]\nmax_workers = \"many\"\n").is_err());
        assert!(Config::parse("[server]\nprot = 1\n").is_err()); // typo'd key
        assert!(Config::parse("[jobs]\nmin_ram = \"2GB\"\n").is_err()); // above max_ram
        assert!(Config::parse("[jobs]\nexpress_when_full = \"later\"\n").is_err());
        assert_eq!(Config::parse("[jobs]\nexpress_when_full = \"shrink\"\n").unwrap().jobs.express_when_full, WhenFull::Shrink);
    }

    #[test]
    fn load_writes_template_once() {
        let dir = std::env::temp_dir().join(format!("tard_test_{}_config", std::process::id()));
        let path = dir.join("config.toml");
        let _ = fs::remove_dir_all(&dir);
        write_template(&path).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), TEMPLATE);
        let _ = fs::remove_dir_all(&dir);
    }
}
