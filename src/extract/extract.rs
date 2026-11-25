// Parallel extraction. One thread walks the archive's headers and sends each entry's
// destination and byte range down a channel, skipping over the contents. Each of the
// -n workers has its own handle on the archive and a buffer of -r / -n bytes: it reads
// the exact bytes of the entries it receives into that buffer and, once the buffer
// passes its high-water mark, writes all of them out. Entries bigger than a buffer are
// streamed through it in buffer-sized pieces.
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::SystemTime,
};
use crossbeam_channel::{Receiver, Sender};
use crate::cli::Args;
use crate::utils::formatting::format_size;
use crate::utils::logger::{LogEntry, LogMsg, LogTx, Logger};
use crate::utils::path::bytes_to_path;

/// A buffer is written out once it's this full (percent).
const HIGH_WATER: usize = 85;
const MIN_BUFFER: usize = 64 << 10;

pub fn extract(args: Args) -> io::Result<()> {
    // validate args
    let input = match args.input_dir.clone() {
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
    let out_path = match args.output_dir.clone() {
        Some(oup) => oup,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no output directory provided")),
    };

    let resume = args.resume;
    let num_workers = args.num_workers.max(1);
    let per_worker = (args.max_ram / num_workers).max(MIN_BUFFER);

    // logger destinations are fixed here, before the reader/workers start.
    // From here on, all output goes through it.
    let dest_name = input.file_stem().unwrap_or_default().to_string_lossy().into_owned();
    let logger = Logger::spawn(args.verbose, args.file_log_path(&dest_name), out_path.clone())?;
    let _ = logger.tx.send(LogMsg::Info(format!("Extracting {} to {}...", input.display(), out_path.display())));
    let _ = logger.tx.send(LogMsg::Note(format!(
        "{} workers with {} buffers (written out at {}% full)", num_workers, format_size(per_worker as u64), HIGH_WATER,
    )));

    // bounded: the reader only needs to stay a little ahead of the workers
    let (entry_tx, entry_rx) = crossbeam_channel::bounded::<Entry>(num_workers * 256);
    let reader_log = logger.tx.clone();
    let (reader_in, reader_out) = (input.clone(), out_path.clone());
    let reader = thread::spawn(move || {
        let input_name = reader_in.display().to_string();
        let mut skipped = 0usize;
        match read_entries(&reader_in, &reader_out, resume, &entry_tx, &mut skipped, &reader_log) {
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

    let dirs = Arc::new(Mutex::new(HashSet::<PathBuf>::new()));
    let workers: Vec<_> = (0..num_workers).map(|_| {
        let w = Worker::new(&input, per_worker, Arc::clone(&dirs), logger.tx.clone());
        let rx = entry_rx.clone();
        thread::spawn(move || w.and_then(|w| w.run(rx)))
    }).collect();
    drop(entry_rx);

    let _ = reader.join();
    for w in workers {
        match w.join() {
            Ok(Ok(())) => (),
            Ok(Err(e)) => logger.fatal(e),
            Err(_) => logger.fatal("Err -- an extract worker panicked".to_string()),
        }
    }

    let _ = logger.tx.send(LogMsg::Note(format!("Log written to {}", logger.log_path.display())));
    let _ = logger.tx.send(LogMsg::Info(format!("Finished extracting {} to {}", input.display(), out_path.display())));
    logger.finish();

    Ok(())
}

/// Where one archived file goes and where its bytes are in the archive.
struct Entry {
    dest: PathBuf,
    offset: u64,
    len: u64,
}

enum Eof {
    Clean,     // archive ended on an entry boundary
    Truncated, // archive ended inside an entry
}

/// Walks the headers, sending each entry's coordinates and seeking past its contents.
fn read_entries(
    input: &Path,
    out_path: &Path,
    resume: bool,
    tx: &Sender<Entry>,
    skipped: &mut usize,
    log: &LogTx,
) -> io::Result<Eof> {
    let file = File::open(input)?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut pos = 0u64;
    loop {
        if pos == file_len {
            return Ok(Eof::Clean);
        }
        let Some(path_bytes) = read_payload_header(&mut reader, file_len, &mut pos)? else {
            return Ok(Eof::Truncated);
        };
        let mut len_buf = [0u8; 8];
        if pos + 8 > file_len {
            return Ok(Eof::Truncated);
        }
        reader.read_exact(&mut len_buf)?;
        pos += 8;
        let len = u64::from_le_bytes(len_buf);
        let offset = pos;
        if offset.checked_add(len).is_none_or(|end| end > file_len) {
            return Ok(Eof::Truncated);
        }
        reader.seek_relative(len as i64)?;
        pos += len;

        let rel = bytes_to_path(path_bytes)?;
        if !is_safe(&rel) {
            let _ = log.send(LogMsg::Error(format!("Err -- skipped {}: path leaves the output directory", rel.display())));
            continue;
        }
        let dest = out_path.join(&rel);
        // With parallel writers an interrupted run can leave several files cut short,
        // anywhere in the archive. A cut-short file is smaller than its entry, so only
        // files of exactly the right size count as done; the rest are written over.
        if resume && fs::metadata(&dest).is_ok_and(|m| m.is_file() && m.len() == len) {
            *skipped += 1;
            continue;
        }
        if tx.send(Entry { dest, offset, len }).is_err() {
            return Ok(Eof::Clean); // workers are gone (a fatal error is on its way)
        }
    }
}

/// Reads a path: 8-byte length then bytes. None if the archive ends inside it.
fn read_payload_header(reader: &mut BufReader<File>, file_len: u64, pos: &mut u64) -> io::Result<Option<Vec<u8>>> {
    if *pos + 8 > file_len {
        return Ok(None);
    }
    let mut len_buf = [0u8; 8];
    reader.read_exact(&mut len_buf)?;
    *pos += 8;
    let len = u64::from_le_bytes(len_buf);
    if pos.checked_add(len).is_none_or(|end| end > file_len) {
        return Ok(None);
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload)?;
    *pos += len;
    Ok(Some(payload))
}

/// Relative and without `..`, so joining it to the output dir stays inside it.
fn is_safe(p: &Path) -> bool {
    p.components().all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
}

struct Worker {
    archive: File,
    buf: Vec<u8>,
    cap: usize,
    pending: Vec<(PathBuf, usize, usize)>, // dest, start and length in buf
    dirs: Arc<Mutex<HashSet<PathBuf>>>,
    log: LogTx,
}

impl Worker {
    fn new(input: &Path, cap: usize, dirs: Arc<Mutex<HashSet<PathBuf>>>, log: LogTx) -> Result<Self, String> {
        let archive = File::open(input).map_err(|e| format!("Err -- could not open {}: {}", input.display(), e))?;
        Ok(Self { archive, buf: Vec::with_capacity(cap), cap, pending: Vec::new(), dirs, log })
    }

    fn run(mut self, rx: Receiver<Entry>) -> Result<(), String> {
        let high_water = self.cap / 100 * HIGH_WATER;
        for e in rx {
            let len = e.len as usize;
            if len > self.cap {
                self.flush()?;
                self.stream(&e)?;
                continue;
            }
            if self.buf.len() + len > self.cap {
                self.flush()?;
            }
            self.read_into_buf(&e)?;
            if self.buf.len() >= high_water {
                self.flush()?;
            }
        }
        self.flush()
    }

    /// Appends the entry's bytes to the buffer. Never reallocates: callers make room first.
    fn read_into_buf(&mut self, e: &Entry) -> Result<(), String> {
        let start = self.buf.len();
        let len = e.len as usize;
        self.buf.resize(start + len, 0);
        self.archive.seek(SeekFrom::Start(e.offset))
            .and_then(|_| self.archive.read_exact(&mut self.buf[start..]))
            .map_err(|err| format!("Err -- could not read {} from the archive: {}", e.dest.display(), err))?;
        self.pending.push((e.dest.clone(), start, len));
        Ok(())
    }

    /// Writes every buffered file, then empties the buffer.
    fn flush(&mut self) -> Result<(), String> {
        for (dest, start, len) in std::mem::take(&mut self.pending) {
            self.make_parent(&dest)?;
            fs::write(&dest, &self.buf[start..start + len])
                .map_err(|e| format!("Err -- could not write {}: {}", dest.display(), e))?;
            self.logged(dest, len as u64);
        }
        self.buf.clear();
        Ok(())
    }

    /// Copies an entry bigger than the buffer through it, a buffer at a time.
    fn stream(&mut self, e: &Entry) -> Result<(), String> {
        self.make_parent(&e.dest)?;
        let werr = |err: io::Error| format!("Err -- could not write {}: {}", e.dest.display(), err);
        let rerr = |err: io::Error| format!("Err -- could not read {} from the archive: {}", e.dest.display(), err);
        let mut out = File::create(&e.dest).map_err(werr)?;
        self.archive.seek(SeekFrom::Start(e.offset)).map_err(rerr)?;
        let mut remaining = e.len;
        while remaining > 0 {
            let n = remaining.min(self.cap as u64) as usize;
            self.buf.resize(n, 0);
            self.archive.read_exact(&mut self.buf).map_err(rerr)?;
            out.write_all(&self.buf).map_err(werr)?;
            remaining -= n as u64;
        }
        self.buf.clear();
        self.logged(e.dest.clone(), e.len);
        Ok(())
    }

    fn make_parent(&self, dest: &Path) -> Result<(), String> {
        let Some(parent) = dest.parent() else { return Ok(()) };
        if self.dirs.lock().unwrap_or_else(|e| e.into_inner()).contains(parent) {
            return Ok(());
        }
        // create_dir_all is fine with another worker creating the same dirs concurrently
        fs::create_dir_all(parent).map_err(|e| format!("Err -- could not create {}: {}", parent.display(), e))?;
        self.dirs.lock().unwrap_or_else(|e| e.into_inner()).insert(parent.to_path_buf());
        Ok(())
    }

    fn logged(&self, path: PathBuf, size: u64) {
        // the file was just written, so its mtime is now
        let _ = self.log.send(LogMsg::Entry(LogEntry { path, size, mtime: SystemTime::now() }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsafe_paths() {
        assert!(is_safe(Path::new("d/f")));
        assert!(!is_safe(Path::new("../f")));
        assert!(!is_safe(Path::new("/etc/passwd")));
        assert!(!is_safe(Path::new("d/../../f")));
    }
}
