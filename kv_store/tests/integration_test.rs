extern crate rand;

use std::thread;
use std::time;
use std::fs;

macro_rules! byte_vec {
    // `()` indicates that the macro takes no argument.
    ($a: expr) => {
        // The macro will expand into the contents of this block.
        String::from($a).into_bytes()
    };
}

fn random_bytes() -> Vec<u8> {
    (0..20).map(|_| rand::random::<u8>()).collect()
}

fn create_kvstore_in_tmp_folder() -> (kv_store::KVStore, String) {
    let test_dir = format!("./tmp-{}/", rand::random::<u64>());

    let lsm_tree = kv_store::KVStore::new(&test_dir);

    (lsm_tree, test_dir)
}

#[test]
fn test_basic() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    kv.set("b", "platan");
    kv.set("c", "poma");
    kv.delete(&byte_vec!("c"));

    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_basic_while_saving_memtable() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    kv.set("b", "platan");
    kv.set("c", "poma");
    kv.delete(&byte_vec!("c"));

    kv.save_memtable();

    // Test while saving memtable
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    kv.wait_for_threads();

    // Test after memtable is on disk
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_delete_after_saving_memtable() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    kv.set("b", "platan");
    kv.set("c", "poma");

    kv.save_memtable();

    kv.delete(&byte_vec!("c"));

    // Test while saving memtable
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    kv.wait_for_threads();

    // Test after memtable is on disk
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_insert_same_key() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));

    kv.set("a", "platan");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("platan")));

    for _ in 0..10_000 {
        kv.set(random_bytes(), random_bytes());
    }

    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("platan")));

    kv.set("a", "ana");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("ana")));

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

