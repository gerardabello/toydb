use crate::domain;

pub struct VecMemTable<Tkey: Ord + Sized + Clone, Tvalue: Sized + Clone> {
    vec: Vec<(Tkey, Tvalue)>,
}

impl<Tkey: Ord + Sized + Clone, Tvalue: Sized + Clone> VecMemTable<Tkey, Tvalue> {
    pub fn new() -> VecMemTable<Tkey, Tvalue> {
        VecMemTable { vec: vec![] }
    }

    fn set(&mut self, key: Tkey, value: Tvalue) {
        self.vec.push((key, value));
    }

    fn get(&self, key: Tkey) -> Option<Tvalue> {
        let pair = self.vec.iter().find(|&x| x.0 == key);
        match pair {
            Some(p) => Some(p.1.clone()),
            None => None,
        }
    }

    pub fn sorted_entries(&self) -> Vec<(Tkey, Tvalue)> {
        let mut ret = self.vec.to_vec();
        ret.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        ret
    }
}

impl domain::MemTable for VecMemTable<Box<[u8]>, Box<[u8]>> {
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let boxed_key = key.into_boxed_slice();
        let boxed_value= value.into_boxed_slice();
        VecMemTable::set(self, boxed_key, boxed_value)
    }

    fn get(&self, key: Vec<u8>) -> Option<Vec<u8>> {
        let boxed_key = key.into_boxed_slice();
        let boxed_value = VecMemTable::get(self, boxed_key);
        match boxed_value {
            Some(v) => Some(v.to_vec()),
            None => None,
        }
    }

    fn sorted_entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let boxed_entries = VecMemTable::sorted_entries(self);
        boxed_entries.into_iter().map(|p| (p.0.to_vec(), p.1.to_vec())).collect()
    }
}
