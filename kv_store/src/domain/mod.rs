mod lsm_tree;

use std::mem;

pub trait MemTable : 'static  + Sync + Send {
    fn new() -> Self;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&mut self, key: &Vec<u8>);
    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>>;
    fn len(&self) -> usize;
    fn sorted_entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)>;
}

pub struct KVStore<T: MemTable> {
    memtable: T,
    lsm_tree: lsm_tree::LSMTree<T>,
}

impl<T: MemTable> KVStore<T> {
    pub fn new(dir: &str) -> KVStore<T> {
        KVStore {
            memtable: T::new(),
            lsm_tree: lsm_tree::LSMTree::new(dir),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.set(key, value);

        if self.memtable.len() > 3000 {
            self.save_memtable();
        }
    }

    pub fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        if let Some(value) = self.memtable.get(key) {
            return Some(value.to_vec());
        };

        self.lsm_tree.get(key)
    }

    pub fn delete(&mut self, key: &Vec<u8>) {
        self.memtable.delete(key)
    }

    pub fn save_memtable(&mut self) {
        let memtable = mem::replace(&mut self.memtable, T::new());

        self.lsm_tree.save_memtable(memtable);
    }
}
