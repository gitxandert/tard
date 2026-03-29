use std::{
    io,
    fs::{self, File},
    path::{Path, PathBuf},
    collections::VecDeque,
    os::unix::ffi::OsStrExt,
    thread::{self, JoinHandle},
    sync::mpsc::{self, SyncSender},
};
use crate::cli::Args;
use crate::utils::formatting::format_size;

pub fn archive(args: Args) -> io::Result<()> {
    // validate args
    let root = match args.input_dir() {
        Some(dir) => dir,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no input directory provided")),
    };
    let output_dir = match args.output_dir() {
        Some(dir) => dir,
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no output directory provided")),
    };
    // create file to write to
    let out_path = {
        let dir_name = root.file_name()
                        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid directory"))?;
        let mut p = output_dir.join(&dir_name);
        p.set_extension("tard");
        p
    };
    let out_file = File::create(&out_path)?;

    // get root parent for formatting paths
    let root_parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;

    // collect all paths
    let mut paths: Vec<PathBuf> = Vec::new();
    println!("Collecting paths...");
    match recurse_dir(root, &mut paths) {
        Ok(_) => println!("Found {} paths", paths.len()),
        Err(e) => {
            println!("Err -- could not recurse {}: {}", root.display(), e);
            std::process::exit(1);
        }
    }

    let num_workers = 8;
    let num_buffers = num_workers * 4;
    let chunk_size = 1024 * 1024;
    let total_size = num_buffers * chunk_size;

    // path_tx is bounded to num_workers: limits how far ahead workers get of the writer.
    let (path_tx, path_rx) = crossbeam_channel::bounded::<PathBuf>(num_workers);
    let (buf_tx, buf_rx) = crossbeam_channel::unbounded::<Vec<u8>>();
    let (large_file_tx, large_file_rx) = crossbeam_channel::unbounded::<LargeFileJob>();
    let (write_tx, write_rx) = mpsc::sync_channel::<Package>(num_workers);

    // Pre-seed path channel; the rest are dispatched by the writer loop as files complete.
    for _ in 0..num_workers.min(paths.len()) {
        path_tx.send(paths.pop().unwrap()).unwrap();
    }

    // Arena allocation: one block carved into num_buffers chunks for regular workers.
    let mut massive_block = Vec::with_capacity(total_size);
    massive_block.resize(total_size, 0u8);
    let mut current_block = massive_block;
    for _ in 0..num_buffers {
        let buffer_data = current_block.split_off(current_block.len() - chunk_size);
        let mut buf = buffer_data;
        buf.clear();
        buf_tx.send(buf).unwrap();
    }

    // Single dedicated buffer for the large-file thread. TardWriter recycles it back
    // via large_buf_tx after each chunk write, so the large-file thread can fill the next.
    let (large_buf_tx, large_buf_rx) = crossbeam_channel::bounded::<Vec<u8>>(1);
    let large_chunk_size = chunk_size * 2;
    let large_buf = Vec::with_capacity(chunk_size);
    large_buf_tx.send(large_buf).unwrap();

    let mut handles = Vec::<JoinHandle<io::Result<()>>>::with_capacity(num_workers);
    for thread_id in 0..num_workers {
        let handle = spawn_archive_thread(
            thread_id,
            write_tx.clone(),
            path_rx.clone(),
            buf_rx.clone(),
            buf_tx.clone(),
            large_file_tx.clone(),
            PathBuf::from(root_parent),
            chunk_size as u64,
        );
        handles.push(handle);
    }
    drop(path_rx);
    drop(buf_rx);
    drop(large_file_tx);

    let large_file_handle = spawn_large_file_thread(
        large_file_rx,
        large_buf_rx,
        buf_tx.clone(),
        write_tx.clone(),
        large_chunk_size as u64,
    );
    drop(write_tx);

    // TardWriter owns both recycle senders: buf_tx for File packages, large_buf_tx for Chunks.
    let mut write_buffer = TardWriter::new(out_file, chunk_size * 10, buf_tx, large_buf_tx);

    // Wrap in Option so we can drop it (closing the channel) once all paths are dispatched.
    let mut path_tx = Some(path_tx);

    for package in write_rx {
        println!("Received package for {}, chunk {}", package.path.display(), package.chunk_id);

        match write_buffer.write_package(package) {
            Ok(completed) => {
                if let Some(tx) = path_tx.as_ref() {
                    for _ in 0..completed {
                        match paths.pop() {
                            Some(p) => {
                                if tx.send(p).is_err() {
                                    path_tx = None;
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    if path_tx.is_some() && paths.len() == 0 {
                        path_tx = None;
                    }
                }
            }
            Err(TardError::Io(e)) => {
                eprintln!("Err -- I/O: {}", e);
                std::process::exit(1);
            }
            Err(TardError::ChannelClosed) => continue,
        }
    }
    // final flush
    write_buffer.flush()?;

    for handle in handles {
        let _ = handle.join().expect("Worker thread panicked");
    }
    large_file_handle.join().expect("Large-file thread panicked")?;

    println!("Finished writing to {}", out_path.display());

    Ok(())
}

fn spawn_archive_thread(
    thread_id: usize,
    write_tx: SyncSender<Package>,
    path_rx: crossbeam_channel::Receiver<PathBuf>,
    buf_rx: crossbeam_channel::Receiver<Vec<u8>>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    large_file_tx: crossbeam_channel::Sender<LargeFileJob>,
    root_parent: PathBuf,
    chunk_size: u64,
) -> JoinHandle<io::Result<()>> {
    use std::io::Read;

    thread::spawn(move || -> io::Result<()> {
        for path in path_rx {
            let mut buf = match buf_rx.recv() {
                Ok(b) => b,
                Err(_) => return Ok(()),
            };

            let mut file = match File::open(&path) {
                Ok(f) => f,
                Err(e) => {
                    println!("Err -- unable to open {}: {}", path.display(), e);
                    buf_tx.send(buf).unwrap();
                    continue;
                }
            };

            let path_rel = path.strip_prefix(&root_parent).unwrap();
            let path_bytes = path_rel.as_os_str().as_bytes();
            let path_len = path_bytes.len() as u64;
            for i in 0..8 {
                buf.push((path_len >> (8 * i)) as u8);
            }
            buf.extend_from_slice(path_bytes);

            let file_len = match file.metadata() {
                Ok(m) => m.len(),
                Err(e) => {
                    println!("Err -- problem getting metadata for {}: {}", path.display(), e);
                    buf.clear();
                    buf_tx.send(buf).unwrap();
                    continue;
                }
            };
            for i in 0..8 {
                buf.push((file_len >> (8 * i)) as u8);
            }

            println!("Thread {} opened {} ({})", thread_id, path.display(), format_size(file_len));

            if file_len > chunk_size {
                // Hand off to the large-file thread; it does all the chunking.
                large_file_tx.send(LargeFileJob {
                    buf,
                    file,
                    path,
                    remaining_len: file_len,
                }).unwrap();
            } else {
                match file.by_ref().take(file_len).read_to_end(&mut buf) {
                    Ok(_) => (),
                    Err(e) => {
                        println!("Err -- problem reading from {}: {}", path.display(), e);
                        buf.clear();
                        buf_tx.send(buf).unwrap();
                        continue;
                    }
                }
                write_tx.send(Package::new(buf, path, 0, PackageType::File)).unwrap();
            }
        }

        Ok(())
    })
}

// Receives large-file jobs from workers and chunks them one at a time using a single
// dedicated buffer that TardWriter recycles back after each chunk.
// The worker's buffer (containing the header) is copied into the dedicated buffer so
// that all Chunk packages originate from the same buffer and TardWriter can recycle uniformly.
fn spawn_large_file_thread(
    large_file_rx: crossbeam_channel::Receiver<LargeFileJob>,
    large_buf_rx: crossbeam_channel::Receiver<Vec<u8>>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    write_tx: SyncSender<Package>,
    large_chunk_size: u64,
) -> JoinHandle<io::Result<()>> {
    use std::io::Read;

    thread::spawn(move || -> io::Result<()> {
        for job in large_file_rx {
            let LargeFileJob { buf: mut header_buf, mut file, path, mut remaining_len } = job;
            let mut chunk_id = 0;

            println!("Received large file ({})", path.display());
            // Get the dedicated buffer, copy the header into it, then recycle the
            // worker's buffer immediately so it returns to the regular pool.
            let mut large_buf = match large_buf_rx.recv() {
                Ok(b) => b,
                Err(_) => return Ok(()),
            };
            large_buf.extend_from_slice(&header_buf);
            header_buf.clear();
            if buf_tx.send(header_buf).is_err() { /* do nothing */ }

            // Chunk 0: fill remaining capacity with file content.
            let first_read = remaining_len.min(large_chunk_size - large_buf.len() as u64);
            match file.by_ref().take(first_read).read_to_end(&mut large_buf) {
                Ok(_) => (),
                Err(e) => {
                    eprintln!("Err -- problem reading large file ({}): {}", path.display(), e);
                    large_buf.clear();
                    write_tx.send(Package::new(large_buf, path, chunk_id, PackageType::Chunk(false))).unwrap();
                    continue;
                }
            }
            remaining_len -= first_read;
            write_tx.send(Package::new(large_buf, path.clone(), chunk_id, PackageType::Chunk(remaining_len > 0))).unwrap();
            chunk_id += 1;

            // Chunks 1+: block on large_buf_rx until TardWriter recycles the buffer back.
            while remaining_len > 0 {
                let mut large_buf = match large_buf_rx.recv() {
                    Ok(b) => b,
                    Err(_) => return Ok(()),
                };

                let read_len = remaining_len.min(large_chunk_size);
                match file.by_ref().take(read_len).read_to_end(&mut large_buf) {
                    Ok(_) => (),
                    Err(e) => {
                        eprintln!("Err -- problem reading large file ({}): {}", path.display(), e);
                        large_buf.clear();
                        write_tx.send(Package::new(large_buf, path, chunk_id, PackageType::Chunk(false))).unwrap();
                        break;
                    }
                }
                remaining_len -= read_len;
                write_tx.send(Package::new(large_buf, path.clone(), chunk_id, PackageType::Chunk(remaining_len > 0))).unwrap();
                chunk_id += 1;
            }
        }

        Ok(())
    })
}

fn recurse_dir(
    dir: &Path,
    paths: &mut Vec<PathBuf>,
) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(en) => en,
            Err(e) => {
                eprintln!("Err -- problem with entry: {e}");
                continue;
            }
        };

        let path_buf = entry.path();

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

// enums, structs
//

struct LargeFileJob {
    buf: Vec<u8>,       // worker's buffer with header written in
    file: File,
    path: PathBuf,
    remaining_len: u64, // total bytes to read from file
}

enum PackageType {
    File,
    Chunk(bool), // true = more chunks follow, false = last chunk
}

struct Package {
    data: Vec<u8>,
    path: PathBuf,
    chunk_id: usize,
    package_type: PackageType,
}

impl Package {
    fn new(data: Vec<u8>, path: PathBuf, chunk_id: usize, package_type: PackageType) -> Self {
        Self { data, path, chunk_id, package_type }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

// writer for received Packages
struct TardWriter {
    out_file: File,
    buf: Vec<u8>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>,       // recycles File package buffers
    large_buf_tx: crossbeam_channel::Sender<Vec<u8>>, // recycles Chunk package buffers
    capacity: usize,
    awaiting_chunks: bool,
    file_queue: VecDeque<Package>,
}

impl TardWriter {
    fn new(
        out_file: File,
        capacity: usize,
        buf_tx: crossbeam_channel::Sender<Vec<u8>>,
        large_buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    ) -> Self {
        Self {
            out_file,
            buf: Vec::with_capacity(capacity),
            buf_tx,
            large_buf_tx,
            capacity,
            awaiting_chunks: false,
            file_queue: VecDeque::new(),
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
    fn flush_package_data(&mut self, mut package: Package, is_chunk: bool) -> Result<(), TardError> {
        let mut take = package.len().min(self.space_left());
        self.fill_buffer(&mut package.data, take);
        while package.len() > self.space_left() {
            self.flush()?;
            self.clear();
            take = package.len().min(self.space_left());
            self.fill_buffer(&mut package.data, take);
        }
        println!("Siphoned {}, chunk {}", package.path.display(), package.chunk_id);
        if is_chunk {
            if let Err(_) = self.large_buf_tx.send(package.data) {
                return Err(TardError::ChannelClosed);
            }
        } else {
            if let Err(_) = self.buf_tx.send(package.data) {
                return Err(TardError::ChannelClosed);
            }
        }
        Ok(())
    }

    // Write a single package. File packages are queued while chunks are in flight;
    // the queue is drained once the final chunk (has_more=false) arrives.
    // Returns the number of fully-completed paths written.
    fn write_package(&mut self, package: Package) -> Result<usize, TardError> {
        match package.package_type {
            PackageType::File => {
                if self.awaiting_chunks {
                    self.file_queue.push_back(package);
                    return Ok(0);
                }
                self.flush_package_data(package, false)?;
                Ok(1)
            }
            PackageType::Chunk(has_more) => {
                self.flush_package_data(package, true)?;
                self.awaiting_chunks = has_more;
                if !has_more {
                    let mut completed = 1; // the large file itself
                    while let Some(queued) = self.file_queue.pop_front() {
                        self.flush_package_data(queued, false)?;
                        completed += 1;
                    }
                    Ok(completed)
                } else {
                    Ok(0)
                }
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.out_file.write_all(&self.buf)
    }
}

use std::fmt;
use std::io::Error;

#[derive(Debug)]
enum TardError {
    Io(Error),
    ChannelClosed,
}

impl fmt::Display for TardError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TardError::Io(err) => write!(f, "I/O error: {}", err),
            TardError::ChannelClosed => write!(f, "Channel closed"),
        }
    }
}

impl std::error::Error for TardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TardError::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<Error> for TardError {
    fn from(err: Error) -> Self {
        TardError::Io(err)
    }
}
