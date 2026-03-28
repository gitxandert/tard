use std::{
    thread,
    sync::mpsc,
    fs::{self, File},
    path::{Path, PathBuf},
    io::{self, Read, Write},
    collections::{VecDeque, BTreeMap},
};
use crate::cli::Args;

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
    let parent = root.parent()
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
    let (path_tx, path_rx) = mpsc::channel::<OrdPath>();
    let (buf_tx, buf_rx) = mpsc::channel::<Vec<u8>>();
    let (write_tx, write_rx) = mpsc::sync_channel::<Package>(num_workers);
    
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

    for _ in 0..paths.len() {
        match paths.pop_front() {
            Some(p) => path_tx.send(p).unwrap(),
            None => break,
        }
    }

    /*
    for i in 0..num_workers {
        let write_tx = write_tx.clone();
        let path_rx = path_rx.clone();
        let mem_rx = mem_rx.clone();
        let root_parent = parent.clone();
        let chunk_size = chunk_size.clone();
        
        thread::spawn(move || {
            let mut buf = mem_rx.recv()
                .map_err(|_| "Recycle channel closed")?;
            loop {
                for path in path_rx {
                   let file = match File::open(&path) {
                       Okay(p) => p,
                       Err(e) => {
                           eprintln!(
                               "Err -- problem with {}: {}",
                               path.display(),
                               e
                            );
                            continue;
                        }
                    };
                }
            }
        });
    }
    drop(tx);

    let mut total_physical_size = 0u64;

    for arc_entry in rx {
        
    }
*/
    Ok(())
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
