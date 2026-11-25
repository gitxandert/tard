pub mod client;
pub mod daemon;
pub mod jobs;
pub mod prompt;
pub mod proto;
pub mod queue;
pub mod schedule;
pub mod watch;

use std::{
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use crate::config::tard_dir;

/// ~/.tard/server: token, pid, server.log and (later) job logs.
pub fn server_dir() -> PathBuf {
    tard_dir().join("server")
}

pub fn token_path() -> PathBuf {
    server_dir().join("token")
}

pub fn pid_path() -> PathBuf {
    server_dir().join("pid")
}

/// The daemon's own output: one line per server event.
pub fn server_log_path() -> PathBuf {
    server_dir().join("server.log")
}

/// Creates `dir` (and parents), owner-only on Unix.
fn create_private_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Truncates or creates `path`, owner read/write only on Unix.
fn create_private_file(path: &Path) -> io::Result<File> {
    let mut o = OpenOptions::new();
    o.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600);
    }
    o.open(path)
}
