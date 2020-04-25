use kv_store;

fn main() {
    let mut kv = kv_store::KVStore::new();

    kv.set("a", "mandarina");
    kv.set("b", "platan");

    assert_eq!(kv.get("a").unwrap(), b"mandarina");
    assert_eq!(kv.get("b").unwrap(), b"platan");
}
