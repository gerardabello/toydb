use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use std::io::{self, BufRead, BufReader, Read};

use std::sync::{Arc, Mutex};
use std::thread;

use crate::domain::MemTable;

/*
fn find_value_in_sstable(path: &str, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    find_value(reader, key)
}

fn find_value(reader : impl Read + Seek, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let mut buffer = [0; 32];

    loop {
        reader.read_exact(&mut buffer[..8])?;
        println!("{:?}", buffer);

        if size != 8 {
            break;
        }
    }

    Ok(None)
}
*/

// Max size is 64kB
fn serialize_size(size: u16) -> [u8; 2] {
    size.to_be_bytes()
}

fn serialize_values(values: &[(&Vec<u8>, &Vec<u8>)]) -> Vec<u8> {
    let mut ret = Vec::new();
    for p in values {
        if p.0.len() > u16::max_value() as usize {
            panic!("Key bigger than 64kB");
        }

        if p.1.len() > u16::max_value() as usize {
            panic!("Value bigger than 64kB");
        }

        ret.extend_from_slice(&serialize_size(p.0.len() as u16));
        ret.append(&mut p.0.clone());
        ret.extend_from_slice(&serialize_size(p.1.len() as u16));
        ret.append(&mut p.1.clone());
    }

    ret
}

struct SSTable {
    path: String,
}

pub struct LSMTree {
    sstable_dir: String,

    // Secuential number indicating how many sstables we ATTEPTED to save. As writing to the disk
    // can fail, this number might be bigger than the actual number of tables on disk. As it is
    // used for creating the filename of a new sstable file, there can be missing numbers in the
    // list of files. As we only care about the relative order (and it is maintained) this is no
    // problem.
    sstable_current_index: u32,

    // List of SSTables saved on disk, order should be the same as order of filenames
    sstables: Arc<Mutex<Vec<SSTable>>>,
}

impl LSMTree {
    pub fn new() -> Self {
        let dir = String::from("./sstables");

        if let Err(error) = fs::create_dir(&dir) {
            match error.kind() {
                std::io::ErrorKind::AlreadyExists => println!("sstable folder already exists"),
                _ => panic!(error),
            }
        }

        LSMTree {
            sstables: Arc::new(Mutex::new(Vec::new())),
            sstable_dir: dir,
            sstable_current_index: 0,
        }
    }

    fn generate_new_sstable_path(&mut self) -> String {
        let ret = format!(
            "{}/{}.sstable",
            self.sstable_dir, self.sstable_current_index
        );
        self.sstable_current_index += 1;
        ret
    }

    pub fn save_memtable<T: MemTable + Send + 'static>(&mut self, memtable: T) {
        let path = self.generate_new_sstable_path();
        save_memtable_thread(path, self.sstables.clone(), memtable);
    }
}

fn save_memtable_thread<T: MemTable + Send + 'static>(
    path: String,
    sstables: Arc<Mutex<Vec<SSTable>>>,
    memtable: T,
) {
    thread::spawn(move || {
        let values = memtable.sorted_entries();
        let serialized = serialize_values(&values);

        let mut file = match File::create(&path) {
            Err(e) => panic!(e),
            Ok(file) => file,
        };

        if let Err(e) = file.write_all(&serialized[..]) {
            panic!(e)
        }

        let mut sstables = sstables.lock().unwrap();
        sstables.push(SSTable { path })
    });
}
