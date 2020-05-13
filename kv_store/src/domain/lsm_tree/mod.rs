use std::fs;
use std::fs::File;
use std::io::prelude::*;

use std::panic;

use std::io::{self, BufReader, Read, SeekFrom};

use std::sync::{Arc, RwLock};
use std::thread;

use crate::domain::MemTable;

#[cfg(test)]
mod test;

fn find_value_in_sstable(path: &str, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let result = find_value(&mut reader, key);
    match result {
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        a => a,
    }
}

fn find_value<Tr: Read + Seek>(reader: &mut Tr, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
    // 256 seams a reasonable nubmber to reserve, although values could be as big as
    //     u16::max_value()
    let mut buffer: Vec<u8> = Vec::with_capacity(256);
    buffer.resize(2, 0);

    loop {
        // KEY SIZE
        reader.read_exact(&mut buffer[..2])?;
        let mut key_size_bytes = [0u8; 2];
        key_size_bytes.clone_from_slice(&buffer[..2]);
        let key_size = u16::from_be_bytes(key_size_bytes);

        //KEY
        if buffer.len() < key_size as usize {
            buffer.resize(key_size as usize, 0);
        }
        reader.read_exact(&mut buffer[..(key_size as usize)])?;
        let key_read = &buffer[..(key_size as usize)];
        let key_found = key == key_read;

        // VALUE SIZE
        reader.read_exact(&mut buffer[..2])?;
        let mut value_size_bytes = [0u8; 2];
        value_size_bytes.clone_from_slice(&buffer[..2]);
        let value_size = u16::from_be_bytes(value_size_bytes);

        if key_found {
            // VALUE
            if buffer.len() < value_size as usize {
                buffer.resize(value_size as usize, 0);
            }
            reader.read_exact(&mut buffer[..(value_size as usize)])?;
            let value = buffer[..(value_size as usize)].to_vec();
            return Ok(Some(value));
        }

        // Jump to next key
        reader.seek(SeekFrom::Current(value_size as i64))?;
    }

    //Ok(None)
}

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

pub struct LSMTree<T: MemTable> {
    sstable_dir: String,

    // Secuential number indicating how many sstables we ATTEPTED to save. As writing to the disk
    // can fail, this number might be bigger than the actual number of tables on disk. As it is
    // used for creating the filename of a new sstable file, there can be missing numbers in the
    // list of files. As we only care about the relative order (and it is maintained) this is no
    // problem.
    sstable_current_index: u32,

    tmp_memtable: Arc<RwLock<Option<T>>>,
    // List of SSTables saved on disk, order should be the same as order of filenames
    sstables: Arc<RwLock<Vec<SSTable>>>,
    save_tmp_table_handle: Option<thread::JoinHandle<()>>,
}

impl<T: MemTable> LSMTree<T> {
    pub fn new(dir: &str) -> Self {
        let dir = String::from(dir);

        let mut ret = LSMTree {
            sstables: Arc::new(RwLock::new(Vec::new())),
            sstable_dir: dir.clone(),
            sstable_current_index: 0,
            tmp_memtable: Arc::new(RwLock::new(None)),
            save_tmp_table_handle: None,
        };

        if let Err(error) = fs::create_dir(&dir) {
            match error.kind() {
                std::io::ErrorKind::AlreadyExists => {
                    let mut paths: Vec<String> = fs::read_dir(&dir)
                        .unwrap()
                        .map(|path| path.unwrap().path().to_str().unwrap().to_owned())
                        .collect();
                    paths.sort();

                    println!("sstable folder already exists, loading data");
                    {
                        let mut sstables = ret.sstables.write().unwrap();
                        for path in paths {
                            println!("Found sstable: {}", path);
                            sstables.push(SSTable { path });
                        }
                        ret.sstable_current_index = sstables.len() as u32;
                    }
                    println!("stored data loaded");
                }
                _ => panic!(error),
            };
        }

        ret
    }

    fn generate_new_sstable_path(&mut self) -> String {
        let ret = format!(
            "{}/{:08}.sstable",
            self.sstable_dir, self.sstable_current_index
        );
        self.sstable_current_index += 1;
        ret
    }

    pub fn save_memtable(&mut self, memtable: T) {
        let memtable_lock = Arc::new(RwLock::new(Some(memtable)));
        self.tmp_memtable = memtable_lock.clone();

        let path = self.generate_new_sstable_path();
        self.save_tmp_table_handle = Some(save_memtable_thread(
            path,
            self.sstables.clone(),
            memtable_lock,
        ));
    }

    fn wait_for_threads(&mut self) {
        let handle_opt = std::mem::replace(&mut self.save_tmp_table_handle, None);

        if let Some(handle) = handle_opt {
            let result = handle.join();
            if let Err(e) = result {
                println!("Error in save memtable thread: {:?}", e);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let memtable_result = {
            let memtable = self.tmp_memtable.read().unwrap();
            match &*memtable {
                None => None,
                Some(memtable) => memtable.get(&key.to_vec()).cloned(),
            }
        };

        if let Some(result) = memtable_result {
            return Some(result);
        };

        let sstables = self.sstables.read().unwrap();

        for sstable in (&*sstables).iter().rev() {
            if let Some(value) = find_value_in_sstable(&sstable.path, key).unwrap() {
                return Some(value);
            }
        }
        None
    }
}

impl<T: MemTable> Drop for LSMTree<T> {
    fn drop(&mut self) {
        self.wait_for_threads();
    }
}


fn save_memtable_thread<T: MemTable + Send + Sync + 'static>(
    path: String,
    sstables: Arc<RwLock<Vec<SSTable>>>,
    memtable_lock: Arc<RwLock<Option<T>>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let serialized = {
            let memtable = memtable_lock.read().unwrap();
            let values = match &*memtable {
                Some(memtable) => memtable.sorted_entries(),
                None => panic!("Should have memtable to save"),
            };
            serialize_values(&values)
        };

        let mut file = match File::create(&path) {
            Err(e) => {
                println!("{:?}", e);
                panic!(e);
            },
            Ok(file) => file,
        };

        if let Err(e) = file.write_all(&serialized[..]) {
            panic!(e)
        }

        {
            let mut sstables = sstables.write().unwrap();
            let mut memtable = memtable_lock.write().unwrap();
            sstables.push(SSTable { path });
            *memtable = None;
        }
    })
}
