
pub trait MemTable {
    fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>);
    fn get(&self, key: Box<[u8]>) -> Option<Box<[u8]>>;
    fn sorted_entries(&self) -> Vec<(Box<[u8]>, Box<[u8]>)>;
}



pub struct Domain<T: MemTable> {
    memtable: T,
}

impl<T: MemTable> Domain<T> {
    pub fn new(memtable: T) -> Box<Domain<T>> {
        Box::new(Domain { memtable })
    }

    pub fn get(&self, key: Box<[u8]>) -> Option<Box<[u8]>> {
        self.memtable.get(key)
    }

    fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        self.memtable.set(key, value)
    }
}
