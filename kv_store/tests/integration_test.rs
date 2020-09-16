extern crate rand;

use std::thread;
use std::time::Duration;
use std::fs;

macro_rules! byte_vec {
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

    let kv_store = kv_store::KVStore::new(&test_dir);

    (kv_store, test_dir)
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

    std::mem::drop(kv);
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

    thread::sleep(Duration::from_secs(1));

    // Test after memtable is on disk
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    std::mem::drop(kv);
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

    thread::sleep(Duration::from_secs(1));

    // Test after memtable is on disk
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    std::mem::drop(kv);
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

    std::mem::drop(kv);
    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_persistance() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    for _ in 0..10_000 {
        kv.set(random_bytes(), random_bytes());
    }
    kv.set("b", "gerard");
    kv.set("a", "platan");
    // Drop just after set, to test that memtable is stored to lsm_tree and lsm_tree waits for
    // save thread to finish.
    std::mem::drop(kv);

    let new_kv = kv_store::KVStore::new(&tmp_dir);
    assert_eq!(new_kv.get(&byte_vec!("a")), Some(byte_vec!("platan")));
    assert_eq!(new_kv.get(&byte_vec!("b")), Some(byte_vec!("gerard")));
    std::mem::drop(new_kv);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}
