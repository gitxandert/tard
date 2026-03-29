use std::{
    env,
    path::{Path, PathBuf},
};
use crate::archive::archive::archive;
use crate::extract::extract::extract;

pub fn route_args() -> std::io::Result<()> {
    let args = parse_args()?;
    match args.cmd {
        Cmd::Archive => archive(args),
        Cmd::Extract => extract(args),
    }
}

fn parse_args() -> std::io::Result<Args> {
    let mut args = env::args();
    // skip file
    let _ = args.next();
    let args: Vec<_> = args.collect();
    if args.len() < 2 {
        println!("tard requires an input directory to archive, an output directory in which to place the archive, and an optional command (default -a/--archive [-x/--extract])");
        std::process::exit(1);
    }

    let mut args_struct = Args::new();
    for a in args {
        match a.as_str() {
            "-a" | "--archive" => args_struct.cmd = Cmd::Archive,
            "-x" | "--extract" => args_struct.cmd = Cmd::Extract,
            _ => {
                match args_struct.input_dir {
                    None => {
                        let input_path = PathBuf::from(Path::new(&a));
                        let input_dir = input_path.canonicalize()?;
                        args_struct.input_dir = Some(input_dir);
                    }
                    Some(_) => {
                        match args_struct.output_dir {
                            None => {
                                let output_path = PathBuf::from(Path::new(&a));
                                let output_dir = output_path.canonicalize()?;
                                args_struct.output_dir = Some(output_dir);
                            }
                            Some(_) => {
                                println!("Err: can only specify one input dir and one output dir");
                                std::process::exit(1);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(args_struct)
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
}

impl Args {
    fn new() -> Self {
        Self {
            // default Archive
            cmd: Cmd::Archive,
            input_dir: None,
            output_dir: None,
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
