
pub trait MemTable {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;
    fn sorted_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)>;
}



pub struct Domain<T: MemTable> {
    memtable: T,
}

impl<T: MemTable> Domain<T> {
    pub fn new(memtable: T) -> Box<Domain<T>> {
        Box::new(Domain { memtable })
    }


    pub fn set<Tkey: Into<Vec<u8>>, Tvalue: Into<Vec<u8>>>(&mut self, key: Tkey, value: Tvalue) {
        self.memtable.set(key.into(), value.into())
    }

    pub fn get<Tkey: Into<Vec<u8>>>(&self, key: Tkey) -> Option<Vec<u8>> {
        self.memtable.get(key.into())
    }

    pub fn get_string<Tkey: Into<Vec<u8>>>(&self, key: Tkey) -> Option<String> {
        self.get(key).map(|b| String::from_utf8(b).unwrap())
    }

    /*
    pub fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        self.memtable.get(key)
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.memtable.set(key, value)
    }

    fn set(&mut self, key: String, value: String) {
        self.memtable.set(key.into_bytes(), value.into_bytes())
    }
    */
}
