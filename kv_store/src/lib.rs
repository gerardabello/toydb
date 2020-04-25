mod domain;
mod vec_mem_table;
//mod sstable;

pub struct KVStore {
    kv_store_domain: domain::KVStore<vec_mem_table::VecMemTable<Box<[u8]>, Box<[u8]>>>
}

impl KVStore {
    pub fn new() -> KVStore {
        let memtable = vec_mem_table::VecMemTable::new();

        let kv_store_domain = domain::KVStore::new(memtable);
        KVStore { kv_store_domain }
    }

    pub fn set<Tkey: Into<Vec<u8>>, Tvalue: Into<Vec<u8>>>(&mut self, key: Tkey, value: Tvalue) {
        self.kv_store_domain.set(key.into(), value.into())
    }

    pub fn get<Tkey: Into<Vec<u8>>>(&self, key: Tkey) -> Option<Vec<u8>> {
        self.kv_store_domain.get(key.into())
    }

}

