use crate::domain;

pub struct ArrayMemTable<Tkey: Ord + Sized + Clone, Tvalue: Sized + Clone> {
    arr: Vec<(Tkey, Tvalue)>,
}

impl<Tkey: Ord + Sized + Clone, Tvalue: Sized + Clone> ArrayMemTable<Tkey, Tvalue> {
    pub fn new() -> ArrayMemTable<Tkey, Tvalue> {
        ArrayMemTable { arr: vec![] }
    }

    fn set(&mut self, key: Tkey, value: Tvalue) {
        self.arr.push((key, value));
    }

    fn get(&self, key: Tkey) -> Option<Tvalue> {
        let pair = self.arr.iter().find(|&x| x.0 == key);
        match pair {
            Some(p) => Some(p.1.clone()),
            None => None,
        }
    }

    pub fn sorted_entries(&self) -> Vec<(Tkey, Tvalue)> {
        let mut ret = self.arr.to_vec();
        ret.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        ret
    }
}

impl domain::MemTable for ArrayMemTable<Box<[u8]>, Box<[u8]>> {
    fn set(&mut self, key: Box<[u8]>, value: Box<[u8]>) {
        ArrayMemTable::set(self, key, value)
    }

    fn get(&self, key: Box<[u8]>) -> Option<Box<[u8]>> {
        ArrayMemTable::get(self, key)
    }

    fn sorted_entries(&self) -> Vec<(Box<[u8]>, Box<[u8]>)> {
        ArrayMemTable::sorted_entries(self)
    }
}
