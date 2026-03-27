use std::{
    env, 
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf}
};

fn main() -> io::Result<()> {
    let args: Vec<_> = env::args().collect();
    if args.len() == 1 {
        println!("tard requires a directory path");
        return Ok(());
    }

    let dir = &args[1];
    let dir_path = PathBuf::from(Path::new(dir));

    let canonical = dir_path.canonicalize()?;
    let dir_name = canonical.file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;          
    let out_path = {
        let mut p = PathBuf::from(&dir_name);
        p.set_extension("tard");
        p           
    };  
    let out_file = File::create(&out_path)?;
    let mut writer = BufWriter::new(out_file);
    let mut total_size = 0u64;
    // top-level path uses dir_path as both root and current dir
    recurse_dir(&dir_path, &dir_path, &mut writer, &mut total_size)?;
    
    let _ = writer.flush();

    let out_size = writer
        .into_inner()?
        .metadata()?
        .len();
    
    println!(
        "{} archived from {} to {}", 
        dir_name.display(), 
        format_size(total_size),
        format_size(out_size)
    );
    
    Ok(())
}

fn recurse_dir(
    root: &Path,
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
                let arc = ArcEntry::new(root, &path)?;
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
                recurse_dir(root, &path_buf, writer, total_size)?
            }
        }
    }

    Ok(())
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
    fn new(root: &Path, path: &Path) -> io::Result<Self> {
        let rel_path = path.strip_prefix(root)
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
