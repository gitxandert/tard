use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    io::{self, BufReader, Read},
    time::SystemTime,
};
use crate::cli::Args;
use crate::utils::logger::{LogEntry, LogMsg, Logger};
use crate::utils::path::bytes_to_path;

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

    // logger destinations are fixed here, before the reader/writer start.
    // From here on, all output goes through it.
    let dest_name = input.file_stem().unwrap_or_default().to_string_lossy().into_owned();
    let logger = Logger::spawn(args.verbose, &out_path, &dest_name, out_path.clone())?;
    let _ = logger.tx.send(LogMsg::Info(format!("Extracting {} to {}...", input.display(), out_path.display())));
    let reader_log = logger.tx.clone();
    let input_name = input.display().to_string();
    let reader_in = input.clone();
    let reader_out = out_path.clone();

    let (pckg_tx, pckg_rx) = crossbeam_channel::unbounded::<Package>();
    let pckg_thread = std::thread::spawn(move || {
        let mut skipped = 0usize;
        match read_archive(&reader_in, &reader_out, resume, &pckg_tx, &mut skipped) {
            Ok(Eof::Clean) => (),
            Ok(Eof::Truncated) => {
                let _ = reader_log.send(LogMsg::Error(format!(
                    "Warning -- {} ends mid-entry (truncated?); last entry not extracted", input_name
                )));
            }
            Err(e) => {
                let _ = reader_log.send(LogMsg::Error(format!("Err -- could not read {}: {}", input_name, e)));
            }
        }
        if resume {
            let _ = reader_log.send(LogMsg::Note(format!("Skipped {} files already extracted", skipped)));
        }
    });

    let mut created_dirs = std::collections::HashSet::<PathBuf>::new();

    for Package { dest, content } in pckg_rx {
        let parent = dest.parent().unwrap();
        // borrow for the lookup; only allocate when a new dir is seen
        if !created_dirs.contains(parent) {
            if let Err(e) = fs::create_dir_all(parent) {
                logger.fatal(format!("Err -- could not create {}: {}", parent.display(), e));
            }
            created_dirs.insert(parent.to_path_buf());
        }
        if let Err(e) = fs::write(&dest, &content) {
            logger.fatal(format!("Err -- could not write {}: {}", dest.display(), e));
        }
        // file was just written, so its mtime is now
        let _ = logger.tx.send(LogMsg::Entry(LogEntry {
            path: dest,
            size: content.len() as u64,
            mtime: SystemTime::now(),
        }));
    }

    let _ = pckg_thread.join();

    let _ = logger.tx.send(LogMsg::Note(format!("Log written to {}", logger.log_path.display())));
    let _ = logger.tx.send(LogMsg::Info(format!("Finished extracting {} to {}", input.display(), out_path.display())));
    logger.finish();

    Ok(())
}

enum Eof {
    Clean,     // archive ended on an entry boundary
    Truncated, // archive ended inside an entry
}

fn read_archive(
    input: &Path,
    out_path: &Path,
    resume: bool,
    pckg_tx: &crossbeam_channel::Sender<Package>,
    skipped: &mut usize,
) -> io::Result<Eof> {
    let mut reader = BufReader::new(File::open(input)?);
    // Files are extracted in archive order, so an interrupted run leaves at most one
    // half-written file: the last one that exists before the first missing one.
    // It's held back here and rewritten instead of skipped.
    let mut last_existing: Option<Package> = None;
    let rewrite = |last: &mut Option<Package>, skipped: &mut usize| {
        if let Some(p) = last.take() {
            *skipped -= 1;
            let _ = pckg_tx.send(p);
        }
    };
    let mut boundary_found = false;
    loop {
        let path = match parse_path(&mut reader) {
            Ok(p) => p,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                rewrite(&mut last_existing, skipped); // interrupted on the very last file
                return Ok(Eof::Clean);
            }
            Err(e) => return Err(e),
        };

        let content = match parse_content(&mut reader) {
            Ok(c) => c,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                rewrite(&mut last_existing, skipped);
                return Ok(Eof::Truncated);
            }
            Err(e) => return Err(e),
        };

        let dest = out_path.join(&path);
        if resume && dest.exists() {
            *skipped += 1;
            if !boundary_found {
                last_existing = Some(Package::new(dest, content));
            }
            continue;
        }
        if !boundary_found {
            boundary_found = true;
            rewrite(&mut last_existing, skipped);
        }

        let _ = pckg_tx.send(Package::new(dest, content));
    }
}

fn parse_path(reader: &mut BufReader<File>) -> io::Result<PathBuf> {
    bytes_to_path(get_payload(reader)?)
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
