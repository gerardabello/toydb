mod lsm_tree;

use std::mem;

pub trait MemTable {
    fn new() -> Self;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&mut self, key: &Vec<u8>);
    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>>;
    fn sorted_entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)>;
}

pub struct KVStore<T: MemTable + Sync + Send + 'static> {
    memtable: T,
    lsm_tree: lsm_tree::LSMTree<T>,
}

impl<T: 'static + MemTable + Send + Sync> KVStore<T> {
    pub fn new() -> KVStore<T> {
        KVStore {
            memtable: T::new(),
            lsm_tree: lsm_tree::LSMTree::new("./sstables"),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.set(key, value)
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        self.memtable.get(key)
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        self.memtable.delete(key)
    }

    pub fn save_memtable(&mut self) {
        let memtable = mem::replace(&mut self.memtable, T::new());

        self.lsm_tree.save_memtable(memtable);
    }
}
