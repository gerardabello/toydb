mod lsm_tree;

use std::mem;

pub const TOMBSTONE: [u8; 32] = [
    179, 210, 155, 16, 110, 229, 104, 202, 72, 124, 209, 13, 85, 192, 56, 71, 239, 10, 116, 199,
    186, 205, 163, 143, 3, 43, 125, 16, 157, 22, 47, 244,
];

pub trait MemTable: 'static + Sync + Send {
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

#[cfg(test)]
pub(crate) mod memtable_trait_tests {
    use super::*;

    macro_rules! byte_vec {
        ($a: expr) => {
            String::from($a).into_bytes()
        };
    }

    pub fn test_basic<T: MemTable>(mut memtable: T) {
        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        memtable.set(byte_vec!("b"), byte_vec!("platan"));

        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));
        assert_eq!(memtable.get(&byte_vec!("b")), Some(&byte_vec!("platan")));
        assert_eq!(memtable.get(&byte_vec!("c")), None);
    }

    pub fn test_insert_same_key<T: MemTable>(mut memtable: T) {
        // It should return the last element added with a given key

        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));

        memtable.set(byte_vec!("a"), byte_vec!("platan"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("platan")));

        memtable.set(byte_vec!("a"), byte_vec!("ana"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("ana")));

        memtable.set(byte_vec!("a"), byte_vec!("zzz"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("zzz")));
    }

    pub fn test_delete<T: MemTable>(mut memtable: T) {
        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));

        memtable.delete(&byte_vec!("a"));
        assert_eq!(memtable.get(&byte_vec!("a")), None);
    }

    pub fn test_sorted_entries<T: MemTable>(mut memtable: T) {
        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        memtable.delete(&byte_vec!("a"));

        memtable.set(byte_vec!("b"), byte_vec!("yyyy"));
        memtable.set(byte_vec!("b"), byte_vec!("zzzz"));
        memtable.set(byte_vec!("d"), byte_vec!("ana"));
        memtable.set(vec![1, 2, 3], byte_vec!("3 numeros"));
        memtable.set(vec![2, 3], byte_vec!("2 numeros"));
        memtable.set(vec![99, 3], byte_vec!("la c"));

        assert_eq!(
            memtable.sorted_entries(),
            vec![
                (&vec![1, 2, 3], &byte_vec!("3 numeros")),
                (&vec![2, 3], &byte_vec!("2 numeros")),
                (&byte_vec!("a"), &TOMBSTONE.to_vec()),
                (&byte_vec!("b"), &byte_vec!("zzzz")),
                (&vec![99, 3], &byte_vec!("la c")),
                (&byte_vec!("d"), &byte_vec!("ana")),
            ]
        );
    }
}
