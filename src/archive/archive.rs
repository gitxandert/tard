use std::{
    io,
    path::{Path, PathBuf},
    os::unix::ffi::{OsStrExt, OsStringExt},
    thread::{self, JoinHandle},
    fs::{self, File, OpenOptions},
};
use crate::cli::Args;
use crate::utils::formatting::format_size;

pub fn archive(args: Args) -> io::Result<()> {
    println!("Archiving...");
    // validate args
    let input_dir = match args.input_dir {
        Some(ref dir) => dir.clone(),
        None => return Err(io::Error::new(io::ErrorKind::InvalidInput, "no input directory provided")),
    };
    let root = args.input_dir().unwrap();
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
    let mut out_file = OpenOptions::new()
        .read(args.resume)
        .write(true)
        .append(args.resume)
        .create(true)
        .open(&out_path)?;

    // get root parent for formatting paths
    let root_parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;

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
        Some(ExistingPaths::new(&mut out_file, root_parent))
    } else {
        None
    };

    if let Some(ref paths) = existing_paths {
        println!("Found {} encoded paths", paths.paths.len());
    }

    // separate thread for collecting paths
    let path_collector = thread::spawn(move || -> io::Result<()> {
        recurse_dir(&input_dir, path_tx, existing_paths.as_mut())
    });

    println!("Allocating {}...", format_size(total_size as u64));
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

    let mut handles = Vec::<JoinHandle<io::Result<()>>>::with_capacity(num_workers);
    for thread_id in 0..num_workers {
        let handle = spawn_archive_thread(
            thread_id,
            write_tx.clone(),
            path_rx.clone(),
            buf_rx.clone(),
            buf_tx.clone(),
            PathBuf::from(root_parent),
            chunk_size as u64,
        );
        handles.push(handle);
    }
    drop(path_rx);
    drop(buf_rx);
    drop(write_tx);

    let mut write_buffer = TardWriter::new(out_file, ram_split, buf_tx);

    for package in write_rx {
        println!("Received package for {}", package.path.display());

        match write_buffer.write_package(package) {
            Ok(()) => continue,
            Err(e) => {
                eprintln!("Err -- I/O: {}", e);
                std::process::exit(1);
            }
        }
    }
    // final flush
    write_buffer.flush()?;

    let _ = path_collector.join().expect("Path collector panicked");
    for handle in handles {
        let _ = handle.join().expect("Worker thread panicked");
    }

    println!("Finished writing to {}", out_path.display());

    Ok(())
}

fn spawn_archive_thread(
    thread_id: usize,
    write_tx: crossbeam_channel::Sender<Package>,
    path_rx: crossbeam_channel::Receiver<PathData>,
    buf_rx: crossbeam_channel::Receiver<Vec<u8>>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    root_parent: PathBuf,
    chunk_size: u64,
) -> JoinHandle<io::Result<()>> {
    use std::io::Read;

    thread::spawn(move || -> io::Result<()> {
        for path_data in path_rx {
            let mut buf = match buf_rx.recv() {
                Ok(b) => b,
                Err(_) => return Ok(()),
            };

            let path = path_data.path;
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

            let file_len = path_data.len;
            for i in 0..8 {
                buf.push((file_len >> (8 * i)) as u8);
            }

            println!("Thread {} opened {} ({})", thread_id, path.display(), format_size(file_len));

            if file_len > chunk_size {
                // send the entire file to the write thread
                write_tx.send(Package::new(buf, path, PackageType::File(file, file_len))).unwrap();
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
                write_tx.send(Package::new(buf, path, PackageType::Chunk)).unwrap();
            }
        }

        Ok(())
    })
}

fn recurse_dir(
    dir: &Path,
    path_rx: crossbeam_channel::Sender<PathData>,
    mut existing: Option<&mut ExistingPaths>,
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
                if let Some(ref mut ex) = existing {
                    if ex.check(&path_buf) {
                        println!("Skipping encoded file {}", path_buf.display());
                        continue;
                    }
                }
                let len = metadata.len();
                if path_rx.send(PathData::new(path_buf, len)).is_err() {
                    return Ok(());  // channel is closed
                }
            } else if metadata.is_dir() {
                match recurse_dir(&path_buf, path_rx.clone(), existing.as_deref_mut()) {
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

struct ExistingPaths {
    paths: Vec<PathBuf>,
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

            let rel_path = PathBuf::from(std::ffi::OsString::from_vec(path_bytes));
            paths.push(root_parent.join(rel_path));
            last_header_pos = pos;
        }

        // remove last path and truncate file to erase its header + content
        if !paths.is_empty() {
            paths.pop();
            let _ = out_file.set_len(last_header_pos);
            let _ = out_file.seek(SeekFrom::End(0));
        }

        Self { paths }
    }

    fn check(&mut self, path: &Path) -> bool {
        for i in 0..self.paths.len() {
            if path == self.paths[i] {
                let _ = self.paths.remove(i);
                return true;
            }
        }
        return false;
    }
}

struct PathData {
    path: PathBuf,
    len: u64,
}

impl PathData {
    fn new(path: PathBuf, len: u64) -> Self {
        Self { path, len }
    }
}

enum PackageType {
    Chunk,           // extracted contents of small file
    File(File, u64), // large file handle and length
}

struct Package {
    data: Vec<u8>,
    path: PathBuf,
    package_type: PackageType,
}

impl Package {
    fn new(data: Vec<u8>, path: PathBuf, package_type: PackageType) -> Self {
        Self { data, path, package_type }
    }
}

// writer for received Packages
struct TardWriter {
    out_file: File,
    buf: Vec<u8>,
    buf_tx: crossbeam_channel::Sender<Vec<u8>>, // recycles File package buffers
    capacity: usize,
}

impl TardWriter {
    fn new(
        out_file: File,
        capacity: usize,
        buf_tx: crossbeam_channel::Sender<Vec<u8>>,
    ) -> Self {
        Self {
            out_file,
            buf: Vec::with_capacity(capacity),
            buf_tx,
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
    fn flush_package_buffer(&mut self, path: &Path, mut p_buffer: Vec<u8>) -> io::Result<()> {
        let mut take = p_buffer.len().min(self.space_left());
        self.fill_buffer(&mut p_buffer, take);
        while p_buffer.len() > self.space_left() {
            self.flush()?;
            self.clear();
            take = p_buffer.len().min(self.space_left());
            self.fill_buffer(&mut p_buffer, take);
        }
        println!("Siphoned buffer for {}", path.display());
        if self.buf_tx.send(p_buffer).is_err() { /* do nothing */ }
        Ok(())
    }

    fn flush_package_file(&mut self, package: Package) -> io::Result<()> {
        use std::io::Read;
        
        self.flush_package_buffer(&package.path, package.data)?;
        let PackageType::File(mut file, mut file_len) = package.package_type else { 
            return Ok(()); 
        };
        let mut take = file_len.min(self.space_left() as u64);
        file_len -= take;
        match file.by_ref().take(take).read_to_end(&mut self.buf) {
            Ok(_) => (),
            Err(e) => {
                println!("Err -- problem reading from {}: {}", package.path.display(), e);
                return Ok(());
            }
        }

        while file_len > 0 {
            self.flush()?;
            self.clear();
            take = file_len.min(self.space_left() as u64);
            file_len -= take;
            match file.by_ref().take(take).read_to_end(&mut self.buf) {
                Ok(_) => (),
                Err(e) => {
                    println!("Err -- problem reading from {}: {}", package.path.display(), e);
                    return Ok(());
                }
            }
        }

        println!("Siphoned large file {}", package.path.display());
        Ok(())
    }

    fn write_package(&mut self, package: Package) -> io::Result<()> {
        match package.package_type {
            PackageType::Chunk => {
                self.flush_package_buffer(&package.path, package.data)
            }
            PackageType::File(_, _) => {
                self.flush_package_file(package)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        use std::io::Write;
        self.out_file.write_all(&self.buf)
    }
}
