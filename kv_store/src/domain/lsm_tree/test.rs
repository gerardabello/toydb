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

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.vec.retain(|p| p.0 != key);
        self.vec.push((key, value));
    }

    fn get(&self, key: &Vec<u8>) -> Option<&Vec<u8>> {
        let pair = self.vec.iter().find(|&x| x.0 == *key);
        match pair {
            Some(p) => Some(&p.1),
            None => None,
        }
    }

    fn len(&self) -> usize {
        self.vec.len()
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
}

#[test]
fn test_save_and_get() {
    let (mut lsm_tree, tmp_dir) = create_lsm_tree_in_tmp_folder();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("a"), byte_vec!("a")),
            (byte_vec!("1"), byte_vec!("1")),
            (byte_vec!("fruita"), byte_vec!("poma")),
            (byte_vec!("ciutat"), byte_vec!("Barcelona city")),
            (byte_vec!("2"), byte_vec!("2")),
            (byte_vec!("3"), byte_vec!("3")),
        ],
    );

    dbg!("Getting value fruita");
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
        byte_vec!("Barcelona city")
    );

    assert_eq!(lsm_tree.get(&byte_vec!("mandarina")), None);

    std::mem::drop(lsm_tree);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}


#[test]
fn test_save_waits_for_previous_save() {
    let (mut lsm_tree, tmp_dir) = create_lsm_tree_in_tmp_folder();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("poma")),
            (byte_vec!("ciutat"), byte_vec!("Barcelona city")),
        ],
    );
    // This should wait for the previous save to finish.
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

    std::mem::drop(lsm_tree);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}




#[test]
fn test_merge_tables_while_saving() {
    let (mut lsm_tree, tmp_dir) = create_lsm_tree_in_tmp_folder();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("poma")),
            (byte_vec!("nom"), byte_vec!("Gerard")),
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
    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("mandarina")),
            (byte_vec!("ciutat"), byte_vec!("Sabadell")),
        ],
    );
    lsm_tree.merge_sstables();

    lsm_tree.wait_for_threads();

    // As we called merge_sstables just before saving one, only the committed ones (first two) will
    // actually be merged, so we would end up with the merged table and the newest one.
    assert_eq!(
        lsm_tree.len(),
        2
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("fruita"))
            .expect("Value should be found"),
        byte_vec!("mandarina")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("ciutat"))
            .expect("Value should be found"),
        byte_vec!("Sabadell")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("cotxe"))
            .expect("Value should be found"),
        byte_vec!("Honda")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("nom"))
            .expect("Value should be found"),
        byte_vec!("Gerard")
    );

    assert_eq!(lsm_tree.get(&byte_vec!("coffee")), None);

    std::mem::drop(lsm_tree);

    //fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_merge_tables_while_not_saving() {
    let (mut lsm_tree, tmp_dir) = create_lsm_tree_in_tmp_folder();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("poma")),
            (byte_vec!("nom"), byte_vec!("Gerard")),
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

    lsm_tree.wait_for_threads();

    lsm_tree.merge_sstables();

    lsm_tree.wait_for_threads();

    add_sstable_to_tree(
        &mut lsm_tree,
        vec![
            (byte_vec!("fruita"), byte_vec!("mandarina")),
            (byte_vec!("ciutat"), byte_vec!("Sabadell")),
        ],
    );

    // As we called merge_sstables just before saving one, only the committed ones (first two) will
    // actually be merged, so we would end up with the merged table and the newest one.
    assert_eq!(
        lsm_tree.len(),
        1
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("fruita"))
            .expect("Value should be found"),
        byte_vec!("mandarina")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("ciutat"))
            .expect("Value should be found"),
        byte_vec!("Sabadell")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("cotxe"))
            .expect("Value should be found"),
        byte_vec!("Honda")
    );

    assert_eq!(
        lsm_tree
            .get(&byte_vec!("nom"))
            .expect("Value should be found"),
        byte_vec!("Gerard")
    );

    assert_eq!(lsm_tree.get(&byte_vec!("coffee")), None);

    std::mem::drop(lsm_tree);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}
