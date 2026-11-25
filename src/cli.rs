use std::{
    env,
    ffi::OsString,
    path::{Path, PathBuf},
};
use crate::archive::archive::archive;
use crate::extract::extract::extract;
use crate::utils::formatting::{format_size, parse_size};

pub fn route_args() {
    let mut raw = env::args_os().skip(1);
    if raw.next().is_some_and(|a| a == "server") {
        std::process::exit(crate::server::client::main(raw.collect()));
    }

    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("Error while parsing args: {}", e);
            std::process::exit(2);
        }
    };

    match args.cmd {
        Cmd::Archive => {
            match archive(args) {
                Ok(()) => (),
                Err(e) => {
                    eprintln!("Error while archiving: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Cmd::Extract => {
            match extract(args) {
                Ok(()) => (),
                Err(e) => {
                    eprintln!("Error while extracting: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Cmd::Help => print_help(),
    }
}

fn parse_args() -> Result<Args, String> {
    parse_args_from(env::args_os().skip(1))
}

/// Parses tard's job arguments (no program name). Paths are made absolute against
/// the current directory, so they must exist.
pub fn parse_args_from<I: IntoIterator<Item = OsString>>(args: I) -> Result<Args, String> {
    let mut args = args.into_iter();
    let mut args_struct = Args::new();
    while let Some(a) = args.next() {
        match a.to_str().unwrap_or("") {
            "-h" | "--help" => {
                args_struct.cmd = Cmd::Help;
                return Ok(args_struct);
            }
            "-a" | "--archive" => args_struct.cmd = Cmd::Archive,
            "-x" | "--extract" => args_struct.cmd = Cmd::Extract,
            "-c" | "--continue"  => args_struct.resume = true,
            "-v" | "--verbose" => args_struct.verbose = true,
            "-r" | "--ram" => {
                let v = args.next().ok_or("unspecified value for -r/--ram")?;
                args_struct.max_ram = parse_size(&v.to_string_lossy())?;
                args_struct.ram_given = true;
            }
            "-n" | "--num_workers" => {
                let v = args.next().ok_or("unspecified value for -n/--num_workers")?;
                args_struct.num_workers = match v.to_string_lossy().parse::<usize>() {
                    Ok(0) => return Err("-n/--num_workers must be at least 1".to_string()),
                    Ok(d) => d,
                    Err(e) => return Err(format!("-n/--num_workers: {}", e)),
                };
                args_struct.workers_given = true;
            }
            _ => {
                let path = PathBuf::from(&a).canonicalize()
                    .map_err(|e| format!("{}: {}", Path::new(&a).display(), e))?;
                if args_struct.input_dir.is_none() {
                    args_struct.input_dir = Some(path);
                } else if args_struct.output_dir.is_none() {
                    args_struct.output_dir = Some(path);
                } else {
                    return Err("can only specify one input dir and one output dir".to_string());
                }
            }
        }
    }

    Ok(args_struct)
}

/// A size as -r takes it, in the largest unit that keeps it exact: "64MB", "1536KB".
fn size_arg(n: usize) -> String {
    for (suffix, shift) in [("TB", 40), ("GB", 30), ("MB", 20), ("KB", 10)] {
        if n != 0 && n % (1usize << shift) == 0 {
            return format!("{}{}", n >> shift, suffix);
        }
    }
    n.to_string()
}

fn print_help() {
    let d = Args::new();
    println!(
"Usage: tard [OPTIONS] <INPUT> <OUTPUT_DIR>

Archive a directory into <OUTPUT_DIR>/<name>.tard, or extract a .tard file.

Options:
  -a, --archive              Archive INPUT directory (default)
  -x, --extract              Extract INPUT .tard file
  -c, --continue             Resume an interrupted run (default: {})
  -r, --ram <SIZE>           Max RAM for buffers, e.g. 512KB, 64MB, 1GB (default: {})
  -n, --num_workers <N>      Number of worker threads (default: {})
  -v, --verbose              Also print each file's path and size, plus progress
                             notes. A CSV log is always written to
                             <OUTPUT_DIR>/<name>_<datetime>.log (default: {})
  -h, --help                 Show this help",
        d.resume,
        format_size(d.max_ram as u64),
        d.num_workers,
        d.verbose,
    );
}

// enums, structs
//

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Cmd {
    Archive,
    Extract,
    Help,
}

#[derive(Clone, Debug)]
pub struct Args {
    pub cmd: Cmd,
    pub input_dir: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub max_ram: usize,
    pub num_workers: usize,
    pub resume: bool,
    pub verbose: bool,
    /// -r / -n were given explicitly (the server sizes jobs without them)
    pub ram_given: bool,
    pub workers_given: bool,
}

impl Args {
    fn new() -> Self {
        Self {
            // default to Archive
            cmd: Cmd::Archive,
            input_dir: None,
            output_dir: None,
            max_ram: 1024 * 1024 * 10, // 10MB
            num_workers: 8usize,
            resume: false,
            verbose: false,
            ram_given: false,
            workers_given: false,
        }
    }

    /// The arguments that make `tard` run this job (the inverse of parse_args_from).
    pub fn to_argv(&self) -> Vec<OsString> {
        let mut v: Vec<OsString> = vec![if self.cmd == Cmd::Extract { "-x" } else { "-a" }.into()];
        v.extend(self.input_dir.iter().map(|p| p.clone().into_os_string()));
        v.extend(self.output_dir.iter().map(|p| p.clone().into_os_string()));
        if self.resume { v.push("-c".into()); }
        if self.verbose { v.push("-v".into()); }
        if self.ram_given {
            v.push("-r".into());
            v.push(size_arg(self.max_ram).into());
        }
        if self.workers_given {
            v.push("-n".into());
            v.push(self.num_workers.to_string().into());
        }
        v
    }

    pub fn input_dir(&self) -> Option<&Path> {
        match &self.input_dir {
            Some(dir) => Some(dir),
            None => None,
        }
    }

    pub fn output_dir(&self) -> Option<&Path> {
        match &self.output_dir {
            Some(dir) => Some(dir),
            None => None,
        }
    }
}
