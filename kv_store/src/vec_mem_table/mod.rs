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

    fn delete(&mut self, key: &Tkey) {
        self.vec.retain(|p| p.0 != *key);
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

    fn delete(&mut self, key: &Vec<u8>) {
        VecMemTable::delete(self, key)
    }

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        VecMemTable::get(self, key)
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

    macro_rules! byte_vec {
        ($a: expr) => {
            String::from($a).into_bytes()
        };
    }

    #[test]
    fn test_basic() {
        let mut memtable: VecMemTable<Vec<u8>, Vec<u8>> = VecMemTable::new();

        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        memtable.set(byte_vec!("b"), byte_vec!("platan"));

        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));
        assert_eq!(memtable.get(&byte_vec!("b")), Some(&byte_vec!("platan")));
        assert_eq!(memtable.get(&byte_vec!("c")), None);
    }

    #[test]
    fn test_insert_same_key() {
        // It should return the last element added with a given key

        let mut memtable: VecMemTable<Vec<u8>, Vec<u8>> = VecMemTable::new();

        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));

        memtable.set(byte_vec!("a"), byte_vec!("platan"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("platan")));

        memtable.set(byte_vec!("a"), byte_vec!("ana"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("ana")));

        memtable.set(byte_vec!("a"), byte_vec!("zzz"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("zzz")));
    }

    #[test]
    fn test_delete() {
        let mut memtable: VecMemTable<Vec<u8>, Vec<u8>> = VecMemTable::new();

        memtable.set(byte_vec!("a"), byte_vec!("mandarina"));
        assert_eq!(memtable.get(&byte_vec!("a")), Some(&byte_vec!("mandarina")));

        memtable.delete(&byte_vec!("a"));
        assert_eq!(memtable.get(&byte_vec!("a")), None);
    }

    #[test]
    fn test_sorted_entries() {
        let mut memtable: VecMemTable<Vec<u8>, Vec<u8>> = VecMemTable::new();

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
                (&byte_vec!("b"), &byte_vec!("zzzz")),
                (&vec![99, 3], &byte_vec!("la c")),
                (&byte_vec!("d"), &byte_vec!("ana")),
            ]
        );
    }
}
