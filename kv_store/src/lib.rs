mod domain;
mod vec_mem_table;
//mod sstable;

type MemTable = vec_mem_table::VecMemTable<Box<[u8]>, Box<[u8]>>;
type KVStore = domain::KVStore<MemTable>;

pub fn new() -> KVStore {
    let memtable = vec_mem_table::VecMemTable::new();

    domain::KVStore::new(memtable)
}

