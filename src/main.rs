use kv_store;

fn main() {
    let mut kvStore = kv_store::new();

    kvStore.set("b", "test");

    assert_eq!(kvStore.get_string("a").unwrap(), "mandarina");
    assert_eq!(kvStore.get_string("b").unwrap(), "platan");
}
