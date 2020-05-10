use kv_store;

macro_rules! byte_vec {
    // `()` indicates that the macro takes no argument.
    ($a: expr) => {
        // The macro will expand into the contents of this block.
        String::from($a).into_bytes()
    };
}


#[test]
fn test_basic() {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new();

    kv.set("a", "mandarina");
    kv.set("b", "platan");

    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));
    assert_eq!(kv.get(&byte_vec!("b")), Some(byte_vec!("platan")));
    assert_eq!(kv.get(&byte_vec!("c")), None);
}

#[test]
fn test_insert_same_key() {
    // It should return the last element added with a given key

    let mut kv: kv_store::KVStore = Default::default();

    kv.set("a", "mandarina");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));

    kv.set("a", "platan");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("platan")));

    kv.set("a", "ana");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("ana")));

    kv.set("a", "zzz");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("zzz")));
}

#[test]
fn test_delete() {
    let mut kv: kv_store::KVStore = Default::default();

    kv.set("a", "mandarina");
    assert_eq!(kv.get(&byte_vec!("a")), Some(byte_vec!("mandarina")));

    kv.delete(&byte_vec!("a"));
    assert_eq!(kv.get(&byte_vec!("a")), None);
}
