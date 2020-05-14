mod encoding;

use std::cmp::Ordering;
use std::fs;
use std::fs::File;
use std::io::prelude::*;

use std::panic;

use std::io::{self, BufReader, BufWriter};

use std::sync::{Arc, RwLock};
use std::thread;

use crate::domain::MemTable;

#[cfg(test)]
mod test;

struct SSTable {
    path: String,
}

impl SSTable {
    fn get_reader(&self) -> io::Result<BufReader<File>> {
        let file = File::open(&self.path)?;
        Ok(BufReader::new(file))
    }

    fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let mut reader = self.get_reader()?;
        let result = encoding::find_value(&mut reader, key);
        match result {
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            a => a,
        }
    }
}

impl Clone for SSTable {
    fn clone(&self) -> Self {
        SSTable {
            path: self.path.clone(),
        }
    }
}
impl PartialEq for SSTable {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
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
        self.wait_for_threads();

        let memtable_lock = Arc::new(RwLock::new(Some(memtable)));
        self.tmp_memtable = memtable_lock.clone();

        let path = self.generate_new_sstable_path();
        self.save_tmp_table_handle = Some(save_memtable_thread(
            path,
            self.sstables.clone(),
            memtable_lock,
        ));
    }

    fn merge_memtables(&mut self) {
        let sstables_to_merge = (*self.sstables.read().unwrap()).clone();

        if sstables_to_merge.len() < 2 {
            return;
        }

        // We use the newest (last) sstable in sstables_to_merge as the path for the
        // merged table, so the correct order will be maintained.
        // This path is already used, so the merging operation will have to use a tmp file name
        // until it deletes the merged ones.
        let merged_sstable_path = sstables_to_merge
            .last()
            .expect("At least one sstable")
            .path
            .clone();
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
            if let Some(value) = sstable.get(key).unwrap() {
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
            encoding::serialize_values(&values)
        };

        let mut file = match File::create(&path) {
            Err(e) => {
                println!("{:?}", e);
                panic!(e);
            }
            Ok(file) => file,
        };

        if let Err(e) = file.write_all(&serialized[..]) {
            panic!(e)
        }

        {
            // It's important to write both sstables and tmp_memtable at the same time, so there no
            // point in time where the memtable is dropped and the corresponding sstable is not in
            // the sstables list.
            let mut sstables = sstables.write().unwrap();
            let mut tmp_memtable = memtable_lock.write().unwrap();
            sstables.push(SSTable { path });
            *tmp_memtable = None;
        }
    })
}

fn merge_sstables_thread(
    sstables: Arc<RwLock<Vec<SSTable>>>,
    sstables_to_merge: Vec<SSTable>,
    merged_path: String,
) -> thread::JoinHandle<()> {
    if sstables_to_merge.len() < 2 {
        panic!("Cannot merge less than 2 tables");
    }

    thread::spawn(move || {
        let merged_file =
            File::open(format!("{}.tmp", merged_path)).expect("Should be able to create file");
        let mut writer = BufWriter::new(merged_file);

        let n_tables = sstables_to_merge.len();
        let mut current_key_vec: Vec<Option<Vec<u8>>> = Vec::new();
        current_key_vec.resize(n_tables, None);

        let mut reader_vec: Vec<BufReader<File>> = sstables_to_merge
            .iter()
            .map(|sstable| sstable.get_reader().unwrap())
            .collect();

        let mut buffer: Vec<u8> = Vec::new();

        // Read initial values
        for i in 0..n_tables {
            let key_size_opt = encoding::read_next_datum(&mut reader_vec[i], &mut buffer);
            current_key_vec[i] = match key_size_opt {
                Ok(key_size) => Some(buffer[..key_size].to_vec()),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
                Err(e) => panic!(e),
            }
        }

        loop {
            // FIND INDEXES WITH LOWER KEY
            let first_some_index = current_key_vec.iter().position(|key_opt| key_opt.is_some());

            if first_some_index.is_none() {
                // All keys are None, we finished merging
                break;
            }

            let mut lowest_key: &Vec<u8> = match current_key_vec
                [first_some_index.expect("We already checked that this is not None")]
            {
                Some(ref v) => v,

                // We already checked that first_some_index is an index of a Some
                None => panic!("Should not get here"),
            };

            let mut lowest_key_indexes: Vec<usize> =
                vec![first_some_index.expect("We already checked that this is not None")];

            for (i, current_key_opt) in current_key_vec
                .iter()
                .skip(lowest_key_indexes[0] + 1)
                .enumerate()
            {
                if let Some(current_key) = current_key_opt {
                    match current_key.cmp(lowest_key) {
                        Ordering::Greater => {}
                        Ordering::Equal => lowest_key_indexes.push(i),
                        Ordering::Less => {
                            lowest_key_indexes = vec![i];
                            lowest_key = current_key;
                        }
                    }
                }
            }

            // If multiple ones have the same key, use first in the sstables_to_merge reverse order
            // As current_key_vec has the same order as sstables_to_merge, we just have to take the
            // last element from the lowest_key_indexes.
            let persisted_index: usize = *lowest_key_indexes
                .last()
                .expect("Should have at least one element");

            let persisted_key = lowest_key;
            let persisted_value_size: usize =
                encoding::read_next_datum(&mut reader_vec[persisted_index], &mut buffer)
                    .expect("Every key should have a value");
            let persisted_value = &buffer[..persisted_value_size];

            // Add key+value of lowest to the merged sstable
            // TODO Write persisted_key + persisted_value
            writer
                .write_all(&encoding::serialize_entry(persisted_key, persisted_value))
                .expect("Should be able to write");

            for index in lowest_key_indexes {
                // Skip all values with the same key, as we already got the one we want
                // (persisted_index).
                if index != persisted_index {
                    encoding::skip_next_datum(&mut reader_vec[index], &mut buffer)
                        .expect("Every key should have a value");

                    let key_size_opt =
                        encoding::read_next_datum(&mut reader_vec[index], &mut buffer);
                    current_key_vec[index] = match key_size_opt {
                        Ok(key_size) => Some(buffer[..key_size].to_vec()),
                        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => None,
                        Err(e) => panic!(e),
                    }
                }
            }
        }

        let mut sstables = sstables.write().unwrap();
        sstables.retain(|table| sstables_to_merge.iter().find(|ttm| *ttm == table).is_none());
    })
}
