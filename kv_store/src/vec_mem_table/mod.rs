use crate::domain;

/* TODO replace this with a RBTree-based memtable */
pub struct VecMemTable<Tkey: Ord + Sized, Tvalue: Sized> {
    vec: Vec<(Tkey, Tvalue)>,
}

impl<Tkey: Ord + Sized, Tvalue: Sized> VecMemTable<Tkey, Tvalue> {
    fn new() -> VecMemTable<Tkey, Tvalue> {
        VecMemTable { vec: vec![] }
    }

    fn set(&mut self, key: Tkey, value: Tvalue) {
        self.vec.push((key, value));
    }

    fn delete(&mut self, key: &Tkey) {
        self.vec.retain(|p| p.0 != *key);
    }

    fn get(&self, key: &Tkey) -> Option<&Tvalue> {
        // As we dont replace existing values, just push, we need to search in reverse
        let pair = self.vec.iter().rev().find(|&x| x.0 == *key);
        match pair {
            Some(p) => Some(&p.1),
            None => None,
        }
    }

    fn sorted_entries(&self) -> Vec<&(Tkey, Tvalue)> {
        let mut ret = vec![];
        for i in 0..self.vec.len() {
            let p = &self.vec[i];
            ret.push(p);
        }
        ret.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        ret
    }
}

impl domain::MemTable for VecMemTable<Vec<u8>, Vec<u8>> {
    fn new() -> Self {
        VecMemTable::new()
    }

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        VecMemTable::set(self, key, value)
    }

    fn delete(&mut self, key: &Vec<u8>) {
        VecMemTable::delete(self, key)
    }

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        VecMemTable::get(self, key)
    }

    fn sorted_entries(&self) -> Vec<&(Vec<u8>, Vec<u8>)> {
        VecMemTable::sorted_entries(self)
    }
}
