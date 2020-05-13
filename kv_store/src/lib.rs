mod domain;
mod vec_mem_table;
//mod sstable;

type MemTableType = vec_mem_table::VecMemTable<Vec<u8>, Vec<u8>>;
type DomainKVStoreType = domain::KVStore<MemTableType>;

pub struct KVStore {
    kv_store_domain: DomainKVStoreType,
}

impl<'a> KVStore {
    pub fn new(dir: &str) -> KVStore {
        let kv_store_domain: DomainKVStoreType = domain::KVStore::new(dir);
        KVStore { kv_store_domain }
    }

    pub fn set<Tkey: Into<Vec<u8>>, Tvalue: Into<Vec<u8>>>(&mut self, key: Tkey, value: Tvalue) {
        self.kv_store_domain.set(key.into(), value.into())
    }

    pub fn get<Tkey: Into<&'a Vec<u8>>>(&self, key: Tkey) -> Option<Vec<u8>> {
        self.kv_store_domain.get(key.into())
    }

    pub fn delete<Tkey: Into<&'a Vec<u8>>>(&mut self, key: Tkey) {
        self.kv_store_domain.delete(key.into())
    }

    pub fn save_memtable(&mut self) {
        self.kv_store_domain.save_memtable()
    }

    pub fn wait_for_threads(&mut self) {
        self.kv_store_domain.wait_for_threads()
    }
}
