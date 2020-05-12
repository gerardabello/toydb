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
        self.vec.retain(|p| p.0 != key);
        self.vec.push((key, value));
    }

    fn get(&self, key: &Tkey) -> Option<&Tvalue> {
        let pair = self.vec.iter().find(|&x| x.0 == *key);
        match pair {
            Some(p) => Some(&p.1),
            None => None,
        }
    }

    fn sorted_entries(&self) -> Vec<(&Tkey, &Tvalue)> {
        let mut ret = vec![];
        for i in 0..self.vec.len() {
            let p = &self.vec[i];
            ret.push((&p.0, &p.1));
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

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        let option = VecMemTable::get(self, key);

        if let Some(val) = option {
            if val[..] == domain::TOMBSTONE {
                return None;
            }
        }

        option
    }

    fn sorted_entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        VecMemTable::sorted_entries(self)
    }

    fn len(&self) -> usize {
        self.vec.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        domain::memtable_trait_tests::test_basic(VecMemTable::new());
    }

    #[test]
    fn test_insert_same_key() {
        domain::memtable_trait_tests::test_insert_same_key(VecMemTable::new());
    }

    #[test]
    fn test_sorted_entries() {
        domain::memtable_trait_tests::test_sorted_entries(VecMemTable::new());
    }
}
