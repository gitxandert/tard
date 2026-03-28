use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    io::{self, BufReader, Read},
};
use crate::cli::Args;

pub fn extract(args: Args) -> io::Result<()> {
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

    let length = u64::from_le_bytes(len_bytes) as usize;
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    Ok(payload)
}
