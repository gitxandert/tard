use std::{
    io,
    fs::{self, File},
    path::{Path, PathBuf},
    os::unix::ffi::OsStrExt,
    thread::{self, JoinHandle},
    sync::mpsc::{self, SyncSender},
    collections::{VecDeque, BTreeMap},
};
use crate::cli::Args;
use crate::utils::formatting::format_size;

pub fn archive(args: Args) -> io::Result<()> {
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
    let root_parent = root.parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "root has no parent"))?;
    
    // collect all paths
    let mut paths = OrdPathDeque::new();
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

    // make num_workers and total_size be CLI arguments
    let num_workers = 8;
    let num_buffers = num_workers * 2; // Give some "slack" for the BTreeMap
    let chunk_size = 512 * 1024;
    let total_size = num_buffers * chunk_size;

    // three channels for routing ordered paths, pre-allocated buffers, and packages
    let (path_tx, path_rx) = crossbeam_channel::unbounded::<OrdPath>();
    let (buf_tx, buf_rx) = crossbeam_channel::unbounded::<Vec<u8>>();
    let (write_tx, write_rx) = mpsc::sync_channel::<Package>(num_workers);

    for _ in 0..paths.len() {
        match paths.pop_front() {
            Some(p) => path_tx.send(p).unwrap(),
            None => break,
        }
    }
    drop(path_tx);
    drop(paths);

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
        
        buf_tx.send(buf).unwrap();
    }

    let mut handles = Vec::<JoinHandle<io::Result<()>>>::with_capacity(num_workers);
    for thread_id in 0..num_workers {
        let write_tx = write_tx.clone();
        let path_rx = path_rx.clone();
        let buf_rx = buf_rx.clone();
        let root_parent = PathBuf::from(root_parent);

        let handle = spawn_archive_thread(
            thread_id,
            write_tx,
            path_rx,
            buf_rx,
            root_parent,
            chunk_size as u64
        );

        handles.push(handle);
    }
    drop(write_tx);
    drop(path_rx);
    drop(buf_rx);

    use std::io::Write;

    let mut write_buffer = TardBuffer::new(chunk_size * 4);
    for mut package in write_rx {
        println!("Received package for path {}, chunk {}", package.path_id, package.chunk_id);
        
        while package.len() > write_buffer.space_left() {
            write_buffer.fill(&mut package.data, write_buffer.space_left());
            out_file.write_all(&write_buffer.buf)?;
            write_buffer.clear();
        }

        if let Err(_) = buf_tx.send(package.data) {
            continue;
        }
    }
    
    for handle in handles {
        let _ = handle.join().expect("Worker thread panicked");
    }
    
    println!("Finished writing to {}", out_path.display());

    Ok(())
}

fn spawn_archive_thread(
    thread_id: usize,
    write_tx: SyncSender<Package>,
    path_rx: crossbeam_channel::Receiver<OrdPath>,
    buf_rx: crossbeam_channel::Receiver<Vec<u8>>,
    root_parent: PathBuf,
    chunk_size: u64
) -> JoinHandle<io::Result<()>> {
    use std::io::Read;
    return thread::spawn(move || -> io::Result<()> {
        for path in path_rx {
            // 1. open file
            // 2. get filename (relative to root_parent)
            // 3. get length of filename and file content
            // 4. store filename_length, filename, and content_len as header
            // 5. take from header and file until either:
            //  -a: all content is read
            //  -b: buf is full
            // 6. if all content is read and buf is not full, send Package with is_last set to true
            // 7. if not all content is read and buf is full, send Package with is_last set to false
            //  -- keep reading from file and sending Packages until all content is read
            //  -- increment chunk_id with each Package until the last
            let mut buf = match buf_rx.recv() {
                Ok(b) => b,
                Err(_) => return Ok(()), // Channel closed, exit gracefully
            };
            
            let path_ref = path.refer();
            let mut file = File::open(path_ref)?;
            
            let path_rel = path.path.strip_prefix(&root_parent).unwrap();
            let path_bytes = path_rel.as_os_str().as_bytes();
            let path_len = path_bytes.len() as u64;
            for i in 0..8 {
                buf.push((path_len >> 8 * i) as u8);
            }
            buf.extend_from_slice(path_bytes);
            
            let file_len = file.metadata()?.len();
            for i in 0..8 {
                buf.push((file_len >> 8 * i) as u8);
            }
            
            println!("Thread {} opened path {} ({})", thread_id, path.display(), format_size(file_len));
            
            let mut chunk_id = 0;
            let mut cur_len = file_len;
            let header_len = 16_u64 + path_len;
            let mut read_len = cur_len.min(chunk_size - header_len);
            while cur_len > chunk_size {
                file.by_ref().take(read_len).read_to_end(&mut buf)?;

                write_tx.send(Package::new(buf, path.id, chunk_id, false)).unwrap();

                buf = match buf_rx.recv() {
                    Ok(b) => b,
                    Err(_) => return Ok(()), // Channel closed, exit gracefully
                };

                chunk_id += 1;
                cur_len -= read_len;
                read_len = cur_len.min(chunk_size);
            }

            file.by_ref().take(read_len).read_to_end(&mut buf)?;
            write_tx.send(Package::new(buf, path.id, chunk_id, true)).unwrap();
        }

        Ok(())
    });
}

fn recurse_dir(
    dir: &Path,
    paths: &mut OrdPathDeque
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
                paths.push_back_path(path_buf);
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

// track paths by ID
struct OrdPath {
    path: PathBuf,
    id: usize,
}

impl OrdPath {
    fn new(path: PathBuf, id: usize) -> Self {
        Self { path, id, }
    }

    fn display(&self) -> std::path::Display<'_> {
        self.path.display()
    }

    fn refer(&self) -> &Path {
        &self.path
    }
}

struct OrdPathDeque {
    paths: VecDeque<Option<OrdPath>>,
    capacity: usize,
}

impl OrdPathDeque {
    fn new() -> Self {
        let capacity = 256;
        Self {
            paths: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    fn push_back_path(&mut self, path: PathBuf) {
        let id = self.paths.len();
        if id >= self.capacity {
            self.resize();
        }
        let ordpath = OrdPath::new(path, id);
        self.paths.push_back(Some(ordpath));
    }

    fn pop_front(&mut self) -> Option<OrdPath> {
        self.paths.pop_front().flatten()
    }

    fn resize(&mut self) {
        self.capacity *= 2;
        self.paths.reserve(self.capacity);
    }

    fn len(&self) -> usize {
        self.paths.len()
    }
}

// sequence data by path_id,chunk_id
struct Package {
    data: Vec<u8>,
    path_id: usize,
    chunk_id: usize,
    is_last: bool,
}

impl Package {
    fn new(data: Vec<u8>, path_id: usize, chunk_id: usize, is_last: bool) -> Self {
        Self { data, path_id, chunk_id, is_last }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

// write buffer for received Packages
struct TardBuffer {
    buf: Vec<u8>,
    capacity: usize,
}

impl TardBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            capacity
        }
    }

    fn fill(&mut self, data: &mut Vec<u8>, take: usize) {
        self.buf.extend(data.drain(0..take));
    }

    fn space_left(&self) -> usize {
        self.capacity.saturating_sub(self.buf.len())
    }

    fn clear(&mut self) {
        self.buf.clear();
    }
}
