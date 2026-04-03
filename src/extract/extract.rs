use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    io::{self, BufReader, Read},
};
use crate::cli::Args;

pub fn extract(args: Args) -> io::Result<()> {
    // validate args
    let input = match args.input_dir {
        Some(inp) => inp,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no input directory provided")),
    };
    match input.extension() {
        Some(ext) => {
            match ext.to_str() {
                Some("tard") => (),
                _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "input to tard -x must be .tard file")),
            }
        }
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "input to tard -x must be .tard file")),
    }
    let out_path = match args.output_dir {
        Some(oup) => oup,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no output directory provided")),
    };

    let resume = args.resume;

    let (pckg_tx, pckg_rx) = crossbeam_channel::unbounded::<Package>();
    let pckg_thread = std::thread::spawn(move || -> io::Result<()> {
        let in_file = File::open(input)?;
        let mut reader = BufReader::new(in_file);
        loop {
            let path = match parse_path(&mut reader) {
                Ok(p) => p,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(()); }
                    return Err(e);
                }
            };
            
            let content = match parse_content(&mut reader) {
                Ok(c) => c,
                Err(e) => {
                    if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(()); }
                    return Err(e);
                }
            };

            let dest = out_path.join(&path);
            if resume {
                if dest.exists() { 
                    println!("Skipping {}...", dest.display());
                    continue; 
                }
            }

            let _ = pckg_tx.send(Package::new(dest, content));
        }
    });

    let mut created_dirs = std::collections::HashSet::<PathBuf>::new();

    for pckg in pckg_rx {
        let parent = pckg.dest.parent().unwrap().to_path_buf();
        if !created_dirs.contains(&parent) {
            fs::create_dir_all(&parent)?;
            created_dirs.insert(parent);
        }
        println!("{}", pckg.dest.display());
        fs::write(&pckg.dest, &pckg.content)?;
    }

    let _ = pckg_thread.join();

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

    let length = u64::from_le_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    Ok(payload)
}

struct Package {
    dest: PathBuf,
    content: Vec<u8>,
}

impl Package {
    fn new(dest: PathBuf, content: Vec<u8>) -> Self {
        Self { dest, content }
    }
}
