use crate::domain;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct HashMapMemTable<Tkey: Ord + Sized + Eq + Hash , Tvalue: Sized> {
    hashmap: HashMap<Tkey, Tvalue>,
}

impl<Tkey: Ord + Sized + Eq + Hash, Tvalue: Sized> HashMapMemTable<Tkey, Tvalue> {
    fn new() -> HashMapMemTable<Tkey, Tvalue> {
        HashMapMemTable { hashmap: HashMap::new() }
    }

    fn set(&mut self, key: Tkey, value: Tvalue) {
        self.hashmap.insert(key, value);
    }

    fn get(&self, key: &Tkey) -> Option<&Tvalue> {
        self.hashmap.get(key)
    }

    fn sorted_entries(&self) -> Vec<(&Tkey, &Tvalue)> {
        let mut ret : Vec<(&Tkey, &Tvalue)>= self.hashmap.iter().collect();
        ret.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        ret
    }
}

impl domain::MemTable for HashMapMemTable<Vec<u8>, Vec<u8>> {
    fn new() -> Self {
        HashMapMemTable::new()
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        HashMapMemTable::set(self, key, value)
    }

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        HashMapMemTable::get(self, key)
    }

    fn sorted_entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        HashMapMemTable::sorted_entries(self)
    }

    fn len(&self) -> usize {
        self.hashmap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        domain::memtable_trait_tests::test_basic(HashMapMemTable::new());
    }

    #[test]
    fn test_insert_same_key() {
        domain::memtable_trait_tests::test_insert_same_key(HashMapMemTable::new());
    }

    #[test]
    fn test_sorted_entries() {
        domain::memtable_trait_tests::test_sorted_entries(HashMapMemTable::new());
    }
}
