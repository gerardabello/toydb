mod domain;
mod vec_mem_table;
//mod sstable;

type MemTableType = vec_mem_table::VecMemTable<Box<[u8]>, Box<[u8]>>;
type DomainKVStoreType = domain::KVStore<MemTableType>;

pub struct KVStore {
    kv_store_domain: DomainKVStoreType,
}


impl KVStore {
    pub fn new() -> KVStore {
        let kv_store_domain : DomainKVStoreType = domain::KVStore::new();
        KVStore { kv_store_domain }
    }

    pub fn set<Tkey: Into<Vec<u8>>, Tvalue: Into<Vec<u8>>>(&mut self, key: Tkey, value: Tvalue) {
        self.kv_store_domain.set(key.into(), value.into())
    }

    pub fn get<Tkey: Into<Vec<u8>>>(&self, key: Tkey) -> Option<Vec<u8>> {
        self.kv_store_domain.get(key.into())
    }

}
 
impl Default for KVStore {
    fn default() -> Self {
        Self::new()
    }
}
