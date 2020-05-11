use std::thread;
use std::time::Duration;

fn main() {
    let mut kv : kv_store::KVStore = kv_store::KVStore::new("./tmp-main");

    kv.set("a", "mandarina");
    kv.set("b", "platan");


    assert_eq!(kv.get(&String::from("a").into_bytes()).unwrap(), b"mandarina");
    assert_eq!(kv.get(&String::from("b").into_bytes()).unwrap(), b"platan");

    kv.save_memtable();
    thread::sleep(Duration::from_millis(1000));
}
