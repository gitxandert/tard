use std::{
    env, 
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf}
};

fn main() -> io::Result<()> {
    let args = parse_args()?;

    match args.cmd {
        Cmd::Archive => archive(args),
        Cmd::Extract => extract(args),
    }
}

fn archive(args: Args) -> io::Result<()> {
    let out_path = {
        let dir_name = args.input_dir().file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;
        let output_dir = args.output_dir();
        let mut p = output_dir.join(&dir_name);
        p.set_extension("tard");
        p
    };  
    let out_file = File::create(&out_path)?;
    let mut writer = BufWriter::new(out_file);
    let mut total_size = 0u64;

    let root = args.input_dir();
    let parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;
    
    recurse_dir(parent, root, &mut writer, &mut total_size)?;
    
    let _ = writer.flush();

    let out_size = writer
        .into_inner()?
        .metadata()?
        .len();
    
    println!(
        "{} archived from {} to {}", 
        root.display(), 
        format_size(total_size),
        format_size(out_size)
    );
    
    Ok(())
}

fn recurse_dir(
    root_parent: &Path,
    dir: &Path, 
    writer: &mut BufWriter<File>, 
    total_size: &mut u64
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
        
        if let Ok(metadata) = entry.metadata() {
            if metadata.is_file() {
                let arc = ArcEntry::new(root_parent, &path)?;
                let p_size = arc.physical_size();
                let l_size = arc.logical_size();
                println!(
                    "archiving {} ({} as {})...", 
                    arc.path(), 
                    format_size(p_size),
                    format_size(l_size)
                );
                *total_size += p_size;
                arc.write(writer)?
            } else if metadata.is_dir() {
                let path_buf = PathBuf::from(path);
                recurse_dir(root_parent, &path_buf, writer, total_size)?
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

struct ArcEntry {
    path_string: String,
    path_len: u64,
    path_bytes: Vec<u8>,
    content_len: u64,
    content: Vec<u8>,
    physical_size: u64,
}

impl ArcEntry {
    fn new(root_parent: &Path, path: &Path) -> io::Result<Self> {
        let rel_path = path.strip_prefix(root_parent)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path not under root"))?;

        let path_string = rel_path.to_string_lossy().into_owned();
        let path_bytes = path_string.as_bytes().to_vec();
        let path_len = path_bytes.len() as u64;
        let content = fs::read(path)?;
        let content_len = content.len() as u64;
        let physical_size = Self::get_physical_size(&path)?;
        Ok(Self {
            path_string,
            path_len,
            path_bytes,
            content_len,
            content,
            physical_size
        })
    }

    fn logical_size(&self) -> u64 {
        self.content_len
    }

    fn physical_size(&self) -> u64 {
       self.physical_size 
    }

    fn path(&self) -> &str {
        &self.path_string
    }

    fn write(&self, writer: &mut BufWriter<File>) -> io::Result<()> {
        let path_len_bytes = Self::spit_u64(self.path_len);
        let content_len_bytes = Self::spit_u64(self.content_len);
        
        writer.write_all(&path_len_bytes)?;
        writer.write_all(&self.path_bytes)?;
        writer.write_all(&content_len_bytes)?;
        writer.write_all(&self.content)?;
        
        Ok(())
    }

    fn spit_u64(long: u64) -> [u8; 8] {
        let mut spit = [0u8; 8];
        for i in 0..8 {
            spit[i] = (long >> 8 * (7 - i)) as u8;
        }

        spit
    }

    fn get_physical_size(path: &Path) -> std::io::Result<u64> {
        let metadata = fs::metadata(path)?;
        let logical_size = metadata.len();

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // Unix blocks are traditionally 512 bytes for this API
            Ok(metadata.blocks() * 512)
        }

        #[cfg(windows)]
        {
            // Standard Windows cluster size is 4096 bytes (4KB)
            let cluster_size = 4096;
            if logical_size == 0 {
                Ok(0)
            } else {
                // Round up to the nearest cluster: (size + (cluster - 1)) & !(cluster - 1)
                let occupied = (logical_size + cluster_size - 1) & !(cluster_size - 1);
                Ok(occupied)
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            // Fallback for other OSs (WASM, etc.) just returns logical size
            Ok(logical_size)
        }
    }
}

