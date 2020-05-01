pub trait MemTable {
    fn new() -> Self;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn delete(&mut self, key: &Vec<u8>);
    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>>;
    fn sorted_entries(&self) -> Vec<&(Vec<u8>, Vec<u8>)>;
}

pub struct KVStore<T: MemTable> {
    memtable: T,
}

impl<T: MemTable> KVStore<T> {
    pub fn new() -> KVStore<T> {
        KVStore { memtable: T::new() }
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
}
