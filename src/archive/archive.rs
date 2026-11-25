use std::{
    io,
    path::{Path, PathBuf},
    thread::{self, JoinHandle},
    fs::{self, File, OpenOptions},
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};
use crate::cli::Args;
use crate::utils::formatting::format_size;
use crate::utils::logger::{LogEntry, LogMsg, LogTx, Logger};
use crate::utils::path::{bytes_to_path, path_to_bytes};

pub fn archive(args: Args) -> io::Result<()> {
    // validate args (errors here are returned: the logger doesn't exist yet)
    let input_dir = match args.input_dir {
        Some(ref dir) => dir.clone(),
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no input directory provided")),
    };
    let root = args.input_dir().unwrap();
    let output_dir = match args.output_dir() {
        Some(dir) => dir,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no output directory provided")),
    };
    let dir_name = root.file_name()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;
    let out_path = {
        let mut p = output_dir.join(&dir_name);
        p.set_extension("tard");
        p
    };
    // get root parent for formatting paths
    let root_parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;

    // logger destinations are fixed here, before any worker exists.
    // From here on, all output goes through it.
    let logger = Logger::spawn(
        args.verbose,
        output_dir,
        &dir_name.to_string_lossy(),
        PathBuf::from(root_parent),
    )?;
    let _ = logger.tx.send(LogMsg::Info(format!("Archiving {} to {}...", root.display(), out_path.display())));

    // create file to write to
    let mut out_file = match OpenOptions::new()
        .read(args.resume)
        .write(true)
        .append(args.resume)
        .truncate(!args.resume) // a fresh run over an old, longer archive would leave its tail
        .create(true)
        .open(&out_path)
    {
        Ok(f) => f,
        Err(e) => logger.fatal(format!("Err -- could not open {}: {}", out_path.display(), e)),
    };

    let num_workers = args.num_workers;
    let num_buffers = num_workers * 4;
    let total_size = args.max_ram;
    let ram_split = total_size / 2;
    let chunk_size = ram_split / num_buffers;

    let (path_tx, path_rx) = crossbeam_channel::unbounded::<PathData>();
    let (buf_tx, buf_rx) = crossbeam_channel::unbounded::<Vec<u8>>();
    let (write_tx, write_rx) = crossbeam_channel::unbounded::<Package>();

    // build ExistingPaths before spawning so out_file stays on main thread
    let mut existing_paths = if args.resume {
        let ex = ExistingPaths::new(&mut out_file, root_parent);
        let _ = logger.tx.send(LogMsg::Note(format!(
            "Resuming: {} files already in {}", ex.paths.len(), out_path.display()
        )));
        Some(ex)
    } else {
        None
    };

    // separate thread for collecting paths
    let collector_log = logger.tx.clone();
    let path_collector = thread::spawn(move || -> io::Result<()> {
        let r = recurse_dir(&input_dir, path_tx, existing_paths.as_mut(), &collector_log);
        if let Some(ex) = existing_paths {
            let _ = collector_log.send(LogMsg::Note(format!("Skipped {} files already archived", ex.skipped)));
        }
        r
    });

    // Arena allocation: one block carved into num_buffers chunks for regular workers.
    let mut massive_block = Vec::with_capacity(ram_split);
    massive_block.resize(ram_split, 0u8);
    let mut current_block = massive_block;
    for _ in 0..num_buffers {
        let buffer_data = current_block.split_off(current_block.len() - chunk_size);
        let mut buf = buffer_data;
        buf.clear();
        buf_tx.send(buf).unwrap();
    }
    let _ = logger.tx.send(LogMsg::Note(format!(
        "Allocated {} ({} worker buffers of {} + {} write buffer)",
        format_size(total_size as u64), num_buffers, format_size(chunk_size as u64), format_size(ram_split as u64),
    )));

    let mut handles = Vec::<JoinHandle<io::Result<()>>>::with_capacity(num_workers);
    for _ in 0..num_workers {
        let handle = spawn_archive_thread(
            write_tx.clone(),
            path_rx.clone(),
            buf_rx.clone(),
            buf_tx.clone(),
            logger.tx.clone(),
            PathBuf::from(root_parent),
            chunk_size as u64,
        );
        handles.push(handle);
    }
    drop(path_rx);
    drop(buf_rx);
    drop(write_tx);

    let mut write_buffer = TardWriter::new(
        out_file, ram_split, buf_tx, logger.tx.clone(), out_path.display().to_string(),
    );

    for package in write_rx {
        if let Err(e) = write_buffer.write_package(package) {
            logger.fatal(format!("Err -- could not write {}: {}", out_path.display(), e));
        }
    }
    // final flush
    if let Err(e) = write_buffer.flush() {
        logger.fatal(format!("Err -- could not write {}: {}", out_path.display(), e));
    }

    let _ = path_collector.join().expect("Path collector panicked");
    for handle in handles {
        let _ = handle.join().expect("Worker thread panicked");
    }
    drop(write_buffer);

    let log_path = logger.log_path.clone();
    let _ = logger.tx.send(LogMsg::Note(format!("Log written to {}", log_path.display())));
    let _ = logger.tx.send(LogMsg::Info(format!("Finished archiving {} to {}", root.display(), out_path.display())));
    logger.finish();

    Ok(())
}

fn spawn_archive_thread(
    write_tx: crossbeam_channel::Sender<Package>,
    path_rx: crossbeam_channel::Receiver<PathData>,
    buf_rx: crossbeam_channel::Receiver<Vec<u8>>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    log_tx: LogTx,
    root_parent: PathBuf,
    chunk_size: u64,
) -> JoinHandle<io::Result<()>> {
    thread::spawn(move || -> io::Result<()> {
        for path_data in path_rx {
            let mut buf = match buf_rx.recv() {
                Ok(b) => b,
                Err(_) => return Ok(()),
            };

            let PathData { path, len: file_len, mtime } = path_data;
            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    let _ = log_tx.send(LogMsg::Error(format!("Err -- unable to open {}: {}", path.display(), e)));
                    buf_tx.send(buf).unwrap();
                    continue;
                }
            };

            let path_rel = path.strip_prefix(&root_parent).unwrap();
            let path_bytes = match path_to_bytes(path_rel) {
                Ok(b) => b,
                Err(e) => {
                    let _ = log_tx.send(LogMsg::Error(format!("Err -- unable to archive {}: {}", path.display(), e)));
                    buf_tx.send(buf).unwrap();
                    continue;
                }
            };
            let path_len = path_bytes.len() as u64;
            buf.extend_from_slice(&path_len.to_le_bytes());
            buf.extend_from_slice(&path_bytes);

            buf.extend_from_slice(&file_len.to_le_bytes());

            if file_len > chunk_size {
                // send the entire file to the write thread
                let _ = log_tx.send(LogMsg::Entry(LogEntry { path: path.clone(), size: file_len, mtime }));
                write_tx.send(Package::new(buf, PackageType::File(file, file_len, path))).unwrap();
            } else {
                let size = match read_chunk(&mut file, file_len, &mut buf) {
                    Ok(n) => n,
                    Err(e) => {
                        let _ = log_tx.send(LogMsg::Error(format!("Err -- problem reading from {}: {}", path.display(), e)));
                        buf.clear();
                        buf_tx.send(buf).unwrap();
                        continue;
                    }
                };
                if size != file_len {
                    let _ = log_tx.send(LogMsg::Error(format!(
                        "Warning -- {} shrank while archiving; stored {} of {} bytes",
                        path.display(), size, file_len,
                    )));
                }
                write_tx.send(Package::new(buf, PackageType::Chunk)).unwrap();
                let _ = log_tx.send(LogMsg::Entry(LogEntry { path, size, mtime }));
            }
        }

        Ok(())
    })
}

/// Appends up to `file_len` bytes of `file` to `buf`, whose last 8 bytes must be the
/// content-length header. If the file turned out shorter (it shrank after it was
/// stat'ed), the header is rewritten to the bytes actually read so the archive stays
/// parseable. Returns the stored length.
fn read_chunk(file: &mut File, file_len: u64, buf: &mut Vec<u8>) -> io::Result<u64> {
    use std::io::Read;
    let start = buf.len();
    file.take(file_len).read_to_end(buf)?;
    let n = (buf.len() - start) as u64;
    if n != file_len {
        buf[start - 8..start].copy_from_slice(&n.to_le_bytes());
    }
    Ok(n)
}

fn recurse_dir(
    dir: &Path,
    path_rx: crossbeam_channel::Sender<PathData>,
    mut existing: Option<&mut ExistingPaths>,
    log_tx: &LogTx,
) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(en) => en,
            Err(e) => {
                let _ = log_tx.send(LogMsg::Error(format!("Err -- problem with entry: {e}")));
                continue;
            }
        };

        let path_buf = entry.path();

        if let Ok(metadata) = entry.metadata() {
            if metadata.is_file() {
                if let Some(ref mut ex) = existing {
                    if ex.check(&path_buf) {
                        continue;
                    }
                }
                // metadata comes from the stat read_dir already did; no extra syscall
                let len = metadata.len();
                let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
                if path_rx.send(PathData { path: path_buf, len, mtime }).is_err() {
                    return Ok(());  // channel is closed
                }
            } else if metadata.is_dir() {
                match recurse_dir(&path_buf, path_rx.clone(), existing.as_deref_mut(), log_tx) {
                    Ok(_) => (),
                    Err(e) => {
                        let _ = log_tx.send(LogMsg::Error(format!(
                            "Err -- could not recurse {}: {}",
                            path_buf.display(),
                            e
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

// enums, structs
//

struct ExistingPaths {
    paths: HashSet<PathBuf>,
    skipped: usize,
}

impl ExistingPaths {
    fn new(out_file: &mut File, root_parent: &Path) -> Self {
        use std::io::{Read, Seek, SeekFrom};

        let _ = out_file.seek(SeekFrom::Start(0));
        let mut reader = std::io::BufReader::new(&mut *out_file);
        let mut paths: Vec<PathBuf> = Vec::new();
        let mut last_header_pos: u64 = 0;

        loop {
            let pos = match reader.stream_position() {
                Ok(p) => p,
                Err(_) => break,
            };

            // read 8-byte path_len
            let mut len_buf = [0u8; 8];
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let path_len = u64::from_le_bytes(len_buf);

            // read path_name
            let mut path_bytes = vec![0u8; path_len as usize];
            if reader.read_exact(&mut path_bytes).is_err() {
                break;
            }

            // read 8-byte content_len
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let content_len = u64::from_le_bytes(len_buf);

            // skip content
            if reader.seek(SeekFrom::Current(content_len as i64)).is_err() {
                break;
            }

            let rel_path = match bytes_to_path(path_bytes) {
                Ok(p) => p,
                Err(_) => break,
            };
            paths.push(root_parent.join(rel_path));
            last_header_pos = pos;
        }

        // remove last path and truncate file to erase its header + content
        if !paths.is_empty() {
            paths.pop();
            let _ = out_file.set_len(last_header_pos);
            let _ = out_file.seek(SeekFrom::End(0));
        }

        Self { paths: paths.into_iter().collect(), skipped: 0 }
    }

    // O(1) instead of a linear scan per file
    fn check(&mut self, path: &Path) -> bool {
        let found = self.paths.remove(path);
        self.skipped += found as usize;
        found
    }
}

struct PathData {
    path: PathBuf,
    len: u64,
    mtime: SystemTime,
}

enum PackageType {
    Chunk,                    // extracted contents of small file
    File(File, u64, PathBuf), // large file handle, length, path (for error reports)
}

struct Package {
    data: Vec<u8>,
    package_type: PackageType,
}

impl Package {
    fn new(data: Vec<u8>, package_type: PackageType) -> Self {
        Self { data, package_type }
    }
}

// writer for received Packages
struct TardWriter {
    out_file: File,
    buf: Vec<u8>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>, // recycles File package buffers
    log_tx: LogTx,
    out_name: String, // for "Flushed" notes
    capacity: usize,
}

impl TardWriter {
    fn new(
        out_file: File,
        capacity: usize,
        buf_tx: crossbeam_channel::Sender<Vec<u8>>,
        log_tx: LogTx,
        out_name: String,
    ) -> Self {
        Self {
            out_file,
            buf: Vec::with_capacity(capacity),
            buf_tx,
            log_tx,
            out_name,
            capacity,
        }
    }

    fn fill_buffer(&mut self, data: &mut Vec<u8>, take: usize) {
        self.buf.extend(data.drain(0..take));
    }

    fn space_left(&self) -> usize {
        self.capacity.saturating_sub(self.buf.len())
    }

    fn clear(&mut self) {
        self.buf.clear();
    }

    // Drain a package's data into the write buffer (flushing as needed) then recycle it.
    fn flush_package_buffer(&mut self, mut p_buffer: Vec<u8>) -> io::Result<()> {
        let mut take = p_buffer.len().min(self.space_left());
        self.fill_buffer(&mut p_buffer, take);
        while p_buffer.len() > self.space_left() {
            self.flush()?;
            self.clear();
            take = p_buffer.len().min(self.space_left());
            self.fill_buffer(&mut p_buffer, take);
        }
        if self.buf_tx.send(p_buffer).is_err() { /* do nothing */ }
        Ok(())
    }

    // The header (with file_len) may already be on disk by the time a read comes up
    // short, so it can't be corrected. Instead the missing bytes are zero-filled, like
    // tar does, keeping every later entry where the format expects it.
    fn flush_package_file(&mut self, package: Package) -> io::Result<()> {
        use std::io::Read;

        self.flush_package_buffer(package.data)?;
        let PackageType::File(mut file, file_len, path) = package.package_type else {
            return Ok(());
        };
        let mut remaining = file_len;
        let mut problem: Option<String> = None;
        while remaining > 0 {
            if self.space_left() == 0 {
                self.flush()?;
                self.clear();
            }
            let take = remaining.min(self.space_left() as u64);
            let start = self.buf.len();
            // on error, read_to_end keeps what it read, so count from buf either way
            let r = file.by_ref().take(take).read_to_end(&mut self.buf);
            let n = (self.buf.len() - start) as u64;
            remaining -= n;
            match r {
                Err(e) => { problem = Some(e.to_string()); break; }
                Ok(_) if n < take => { problem = Some("file shrank while archiving".to_string()); break; }
                Ok(_) => (),
            }
        }

        if let Some(why) = problem {
            let missing = remaining;
            while remaining > 0 {
                if self.space_left() == 0 {
                    self.flush()?;
                    self.clear();
                }
                let n = remaining.min(self.space_left() as u64) as usize;
                self.buf.resize(self.buf.len() + n, 0);
                remaining -= n as u64;
            }
            let _ = self.log_tx.send(LogMsg::Error(format!(
                "Warning -- problem reading from {}: {}; zero-filled the last {} of {}",
                path.display(), why, format_size(missing), format_size(file_len),
            )));
        }

        Ok(())
    }

    fn write_package(&mut self, package: Package) -> io::Result<()> {
        match package.package_type {
            PackageType::Chunk => {
                self.flush_package_buffer(package.data)
            }
            PackageType::File(..) => {
                self.flush_package_file(package)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        if self.buf.is_empty() {
            return Ok(());
        }
        self.out_file.write_all(&self.buf)?;
        // once per full write buffer (max_ram / 2), so the format is negligible
        let _ = self.log_tx.send(LogMsg::Note(format!(
            "Flushed {} to {}", format_size(self.buf.len() as u64), self.out_name
        )));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};

    fn temp_file(name: &str, contents: &[u8]) -> (PathBuf, File) {
        let p = std::env::temp_dir().join(format!("tard_test_{}_{}", std::process::id(), name));
        let mut f = File::create(&p).unwrap();
        f.write_all(contents).unwrap();
        (p.clone(), File::open(&p).unwrap())
    }

    #[test]
    fn read_chunk_rewrites_header_when_file_is_short() {
        let (p, mut f) = temp_file("chunk", b"hello");
        let mut buf = 100u64.to_le_bytes().to_vec(); // header claims 100 bytes
        assert_eq!(read_chunk(&mut f, 100, &mut buf).unwrap(), 5);
        assert_eq!(&buf[..8], &5u64.to_le_bytes());
        assert_eq!(&buf[8..], b"hello");
        let _ = fs::remove_file(p);
    }

    #[test]
    fn large_file_short_read_is_zero_filled() {
        let (src, f) = temp_file("large_src", &[7u8; 30]);
        let out_p = std::env::temp_dir().join(format!("tard_test_{}_large_out", std::process::id()));
        let out = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&out_p).unwrap();
        let (buf_tx, _buf_rx) = crossbeam_channel::unbounded();
        let (log_tx, log_rx) = crossbeam_channel::unbounded();
        // capacity 16 forces several flushes mid-file and mid-padding
        let mut w = TardWriter::new(out, 16, buf_tx, log_tx, String::new());
        let header = 100u64.to_le_bytes().to_vec(); // claims 100, file has 30
        w.write_package(Package::new(header, PackageType::File(f, 100, src.clone()))).unwrap();
        w.flush().unwrap();

        let mut written = Vec::new();
        let mut out = w.out_file.try_clone().unwrap();
        out.seek(SeekFrom::Start(0)).unwrap();
        out.read_to_end(&mut written).unwrap();
        assert_eq!(written.len(), 8 + 100);
        assert!(written[8..38].iter().all(|&b| b == 7));
        assert!(written[38..].iter().all(|&b| b == 0));
        assert!(log_rx.try_iter().any(|m| matches!(m, LogMsg::Error(s) if s.contains("zero-filled"))));
        let _ = fs::remove_file(src);
        let _ = fs::remove_file(out_p);
    }
}
