fn main() {
    let mut kv : kv_store::KVStore = Default::default();

    kv.set("a", "mandarina");
    kv.set("b", "platan");


    assert_eq!(kv.get(&String::from("a").into_bytes()).unwrap(), b"mandarina");
    assert_eq!(kv.get(&String::from("b").into_bytes()).unwrap(), b"platan");
}
