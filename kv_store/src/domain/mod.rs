pub trait MemTable {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;
    fn sorted_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)>;
}

pub struct KVStore<T: MemTable> {
    memtable: T,
}

impl<T: MemTable> KVStore<T> {
    pub fn new(memtable: T) -> KVStore<T> {
        KVStore { memtable }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.set(key, value)
    }

    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        self.memtable.get(key)
    }
}
