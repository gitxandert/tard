use std::{
    env,
    path::{Path, PathBuf},
};
use crate::archive::archive::archive;
use crate::extract::extract::extract;

pub fn route_args() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            println!("Error while parsing args: {}", e);
            return;
        }
    };

    match args.cmd {
        Cmd::Archive => {
            match archive(args) {
                Ok(()) => (),
                Err(e) => println!("Error while archiving: {}", e),
            }
        }
        Cmd::Extract => {
            match extract(args) {
                Ok(()) => (),
                Err(e) => println!("Error while extracting: {}", e),
            }
        }
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = env::args();
    // skip file
    let _ = args.next();

    let mut args_struct = Args::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "-a" | "--archive" => args_struct.cmd = Cmd::Archive,
            "-x" | "--extract" => args_struct.cmd = Cmd::Extract,
            "-r" | "--max_ram" => {
                if let Some(a) = args.next() {
                    args_struct.max_ram = match parse_ram_arg(&a) {
                        Ok(ram) => ram,
                        Err(e) => return Err(e),
                    }
                } else {
                    return Err("unspecified value for -r/--max_ram".to_string());
                }
            }
            "-w" | "--num_workers" => {
                if let Some(a) = args.next() {
                    args_struct.num_workers = match a.parse::<usize>() {
                        Ok(d) => d,
                        Err(e) => return Err(e.to_string()),
                    }
                } else {
                    return Err("unspecified value for -w/--num_workers".to_string());
                }
            }
            _ => {
                match args_struct.input_dir {
                    None => {
                        let input_path = PathBuf::from(Path::new(&a));
                        let input_dir = match input_path.canonicalize() {
                            Ok(id) => id,
                            Err(e) => return Err(e.to_string()),
                        };
                        args_struct.input_dir = Some(input_dir);
                    }
                    Some(_) => {
                        match args_struct.output_dir {
                            None => {
                                let output_path = PathBuf::from(Path::new(&a));
                                let output_dir = match output_path.canonicalize() {
                                    Ok(od) => od,
                                    Err(e) => return Err(e.to_string()),
                                };
                                args_struct.output_dir = Some(output_dir);
                            }
                            Some(_) => return Err("can only specify one input dir and one output dir".to_string()),
                        }
                    }
                }
            }
        }
    }

    Ok(args_struct)
}

fn parse_ram_arg(ram: &str) -> Result<usize, String> {
    let suffixes = [
        ("TB", 1usize << 40),
        ("GB", 1usize << 30),
        ("MB", 1usize << 20),
        ("KB", 1usize << 10),
        ("B", 1usize),
    ];

    for (suffix, multiplier) in &suffixes {
        if let Some(num_str) = ram.strip_suffix(suffix) {
            return num_str
                .parse::<usize>()
                .map(|n| n * multiplier)
                .map_err(|_| "cannot coerce to decimal".to_string());
        }
    }

    ram.parse::<usize>()
        .map_err(|_| "invalid byte signifier".to_string())
}


// enums, structs
//

enum Cmd {
    Archive,
    Extract,
}

pub struct Args {
    cmd: Cmd,
    pub input_dir: Option<PathBuf>,
    pub output_dir: Option<PathBuf>,
    pub max_ram: usize,
    pub num_workers: usize,
}

impl Args {
    fn new() -> Self {
        Self {
            // default to Archive
            cmd: Cmd::Archive,
            input_dir: None,
            output_dir: None,
            max_ram: 1024 * 1024 * 10,
            num_workers: 8usize,
        }
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
