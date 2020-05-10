extern crate rand;

use std::fs;

macro_rules! byte_vec {
    // `()` indicates that the macro takes no argument.
    ($a: expr) => {
        // The macro will expand into the contents of this block.
        String::from($a).into_bytes()
    };
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

    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_insert_same_key() {
    // It should return the last element added with a given key

    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));

    kv.set("a", "platan");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("platan")));

    kv.set("a", "ana");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("ana")));

    kv.set("a", "zzz");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("zzz")));

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}

#[test]
fn test_delete() {
    let (mut kv, tmp_dir) = create_kvstore_in_tmp_folder();

    kv.set("a", "mandarina");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));

    kv.delete(&byte_vec!("a"));
    assert_eq!(kv.get(&byte_vec!("a")), None);

    fs::remove_dir_all(tmp_dir).expect("Remove tmp folder");
}
