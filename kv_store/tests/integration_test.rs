use kv_store;

#[test]
fn test_basic() {
    let mut kv : kv_store::KVStore = Default::default();

    kv.set("a", "mandarina");
    kv.set("b", "platan");

    assert_eq!(kv.get("a").unwrap(), b"mandarina");
    assert_eq!(kv.get("b").unwrap(), b"platan");
}
