use std::{
    env,
    thread,
    sync::mpsc,
    fs::{self, File},
    path::{Path, PathBuf},
    io::{self, BufReader, BufWriter, Read, Write},
};

fn main() -> io::Result<()> {
    let args = parse_args()?;

    match args.cmd {
        Cmd::Archive => archive(args),
        Cmd::Extract => extract(args),
    }
}

fn archive(args: Args) -> io::Result<()> {
    // create file to write to
    let out_path = {
        let dir_name = args.input_dir().file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;
        let output_dir = args.output_dir();
        let mut p = output_dir.join(&dir_name);
        p.set_extension("tard");
        p
    };  
    let mut out_file = File::create(&out_path)?;

    // get root and parent (for formatting paths)
    let root = args.input_dir();
    let parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;
    
    // collect all paths
    let mut paths: Vec<PathBuf> = Vec::new();
    println!("Collecting paths...");
    match recurse_dir(root, &mut paths) {
        Ok(_) => println!("Found {} paths", paths.len()),
        Err(e) => {
            println!(
                "Err -- could not recurse {}: {}",
                root.display(),
                e
            );
            std::process::exit(1);
        }
    }


    let (path_tx, path_rx) = mpsc::channel::<PathBuf>();
    let (mem_tx, mem_rx) = mpsc::channel::<Vec<u8>>();
    let (write_tx, write_rx) = mpsc::sync_channel::<Vec<u8>>(num_workers);

    let num_workers = 8;
    let num_buffers = num_workers * 4; // Give some extra "slack" for the BTreeMap
    let chunk_size = 512 * 1024;
    let total_size = num_buffers * chunk_size;

    // arena-type allocation
    // 1. One giant allocation
    let mut massive_block = Vec::with_capacity(total_size);
    // Safety: "initialize" it
    massive_block.resize(total_size, 0u8); 

    // 2. Carve it up into buffers and send to the recycle bin
    let mut current_block = massive_block;
    for _ in 0..num_buffers {
        // Take the last 512KB off the big block
        let buffer_data = current_block.split_off(current_block.len() - chunk_size);
        
        // Convert to a Vec that the workers can use
        // clear() sets length to 0 but keeps the chunk_size capacity
        let mut buf = buffer_data;
        buf.clear(); 
        
        idle_tx.send(buf).unwrap();
    }

    for p in paths {
        path_tx.send(p).unwrap();
    }

    for i in 0..num_workers {
        let write_tx = write_tx.clone();
        let path_rx = path_rx.clone();
        let mem_rx = mem_rx.clone();
        let root_parent = parent.clone();
        
        thread::spawn(move || {
            let mut buf = mem_rx.recv()
                .map_err(|_| "Recycle channel closed")?;
            loop {
                for path in path_rx {
                   let file = match File::open(&path) {
                       Okay(p) => p,
                       Err(e) => {
                           eprintln!(
                               "Err -- problem with {}: {}",
                               path.display(),
                               e
                            );
                            continue;
                        }
                    };
                }
            }
        });
    }
    drop(tx);

    let mut total_physical_size = 0u64;

    for arc_entry in rx {
        
    }

    Ok(())
}

fn recurse_dir(
    dir: &Path,
    paths: &mut Vec<PathBuf>
) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(en) => en,
            Err(e) => {
                eprintln!("Err -- problem with entry: {e}");
                continue;
            }
        };

        let path = entry.path();
        let path_buf = PathBuf::from(path);
        
        if let Ok(metadata) = entry.metadata() {
            if metadata.is_file() {
                paths.push(path_buf);
            } else if metadata.is_dir() {
                match recurse_dir(&path_buf, paths) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!(
                            "Err -- could not recurse {}: {}",
                            path_buf.display(),
                            e
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn extract(args: Args) -> io::Result<()> {
    match args.input_dir().extension() {
        Some(ext) => {
            match ext.to_str() {
                Some("tard") => (),
                _ => {
                    println!("Err: input for -x must be a .tard file");
                    std::process::exit(1);
                }
            }
        }
        None => {
            println!("Err: input for -x must be a .tard file");
            std::process::exit(1);
        }
    }

    let in_file = File::open(args.input_dir())?;
    let mut reader = BufReader::new(in_file);

    let out_path = args.output_dir();
    let mut created_dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    loop {
        let path = match parse_path(&mut reader) {
            Ok(p) => p,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof { break; }
                return Err(e);
            }
        };
        
        let content = match parse_content(&mut reader) {
            Ok(c) => c,
            Err(e) => {
                if e.kind() == io::ErrorKind::UnexpectedEof { break; }
                return Err(e);
            }
        };

        let dest = out_path.join(&path);
        let parent = dest.parent().unwrap().to_path_buf();
        if !created_dirs.contains(&parent) {
            fs::create_dir_all(&parent)?;
            created_dirs.insert(parent);
        }
        println!("{}", dest.display());
        fs::write(&dest, &content)?;
    }

    Ok(())
}

fn parse_path(reader: &mut BufReader<File>) -> io::Result<PathBuf> {
    let path_payload = get_payload(reader)?;

    let path_string = String::from_utf8(path_payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid UTF-8"))?;

    Ok(PathBuf::from(Path::new(&path_string)))
}

fn parse_content(reader: &mut BufReader<File>) -> io::Result<Vec<u8>> {
    get_payload(reader)
}

fn get_payload(reader: &mut BufReader<File>) -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 8];
    reader.read_exact(&mut len_bytes)?;

    let length = u64::from_be_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    Ok(payload)
}

fn parse_args() -> io::Result<Args> {
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

fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// enums, structs
//

enum Cmd {
    Archive,
    Extract,
}

struct Args {
    cmd: Cmd,
    input_dir: Option<PathBuf>,
    output_dir: Option<PathBuf>,
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

    fn input_dir(&self) -> &Path {
        self.input_dir.as_ref().unwrap()
    }

    fn output_dir(&self) -> &Path {
        self.output_dir.as_ref().unwrap()
    }
}

struct Package {
    data: Vec<u8>,
    is_last: bool,
}

