extern crate rand;

use super::*;

#[derive(Debug)]
struct MockMemtable {
    vec: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MemTable for MockMemtable {
    fn new() -> Self {
        MockMemtable { vec: vec![] }
    }

    fn set(&mut self, _key: Vec<u8>, _value: Vec<u8>) {}

    fn get(&self, _key: &Vec<u8>) -> Option<&Vec<u8>> {
        None
    }

    fn len(&self) -> usize {
        0
    }

    fn sorted_entries(&self) -> Vec<(&Vec<u8>, &Vec<u8>)> {
        let mut ret = vec![];
        for i in 0..self.vec.len() {
            let p = &self.vec[i];
            ret.push((&p.0, &p.1));
        }
        ret.sort_by(|p1, p2| p1.0.cmp(&p2.0));
        ret
    }
}

macro_rules! byte_vec {
    ($a: expr) => {
        String::from($a).into_bytes()
    };
}

fn create_lsm_tree_in_tmp_folder() -> (LSMTree<MockMemtable>, String) {
    let test_dir = format!("./tmp-{}/", rand::random::<u64>());

    let lsm_tree = LSMTree::new(&test_dir);

    (lsm_tree, test_dir)
}

fn add_sstable_to_tree(lsm_tree: &mut LSMTree<MockMemtable>, values: Vec<(Vec<u8>, Vec<u8>)>) {
    let memtable = MockMemtable { vec: values };

    lsm_tree.save_memtable(memtable);

    // Wait for save thread to finish
    lsm_tree.wait_for_threads();
}

#[test]
fn test_save_and_get() {
    let (mut lsm_tree, tmp_dir) = create_lsm_tree_in_tmp_folder();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("poma")),
            (byte_vec!("ciutat"), byte_vec!("Barcelona city")),
        ],
    );

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("cotxe"), byte_vec!("Honda")),
            (byte_vec!("ciutat"), byte_vec!("Mataró city")),
        ],
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("fruita"))
            .expect("Value should be found"),
        byte_vec!("poma")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("ciutat"))
            .expect("Value should be found"),
        byte_vec!("Mataró city")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("cotxe"))
            .expect("Value should be found"),
        byte_vec!("Honda")
    );

    assert_eq!(lsm_tree.get(&byte_vec!("mandarina")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}
