extern crate rand;

use std::fs;
use std::time::{Duration, Instant};
use std::mem::forget;
use std::ptr;
use std::thread;

const TMP_DIR: &str = "./tmp-dir";

fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = ptr::read_volatile(&dummy);
        forget(dummy);
        ret
    }
}

fn random_bytes() -> Vec<u8> {
    (0..20).map(|_| rand::random::<u8>()).collect()
}

fn benchmark(name: &str, mut f: impl FnMut()) {
    thread::sleep(Duration::from_millis(10));
    let start = Instant::now();
    f();
    let duration = start.elapsed();
    thread::sleep(Duration::from_millis(10));
    println!("{}: {:?}", name, duration);
}

fn benchmark_kv_store(name: &str, f: fn(&mut kv_store::KVStore) -> ()) {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
    benchmark(name, move || {
        f(&mut kv);
    });
    fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
}

fn benchmark_kv_store_with_setup(
    name: &str,
    setup_f: fn(&mut kv_store::KVStore) -> (),
    f: fn(&mut kv_store::KVStore) -> (),
) {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
    setup_f(&mut kv);
    benchmark(name, move || {
        f(&mut kv);
    });
    fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
}

fn main() {
    benchmark_kv_store("Add 10_000 random values to empty store", |kv| {
        for _ in 0..10_000 {
            kv.set(random_bytes(), random_bytes());
        }
    });

    benchmark_kv_store("Get 10_000 random values in empty store", |kv| {
        for _ in 0..10_000 {
            black_box(kv.get(&random_bytes()));
        }
    });

    benchmark_kv_store_with_setup(
        "Get 100 random values in store with 100 random values",
        |kv| {
            for _ in 0..100 {
                kv.set(random_bytes(), random_bytes());
            }
        },
        |kv| {
            for _ in 0..100 {
                black_box(kv.get(&random_bytes()));
            }
        },
    );

    benchmark_kv_store_with_setup(
        "Get 100 random values in store with 1000 random values",
        |kv| {
            for _ in 0..1000 {
                kv.set(random_bytes(), random_bytes());
            }
        },
        |kv| {
            for _ in 0..100 {
                black_box(kv.get(&random_bytes()));
            }
        },
    );

    benchmark_kv_store_with_setup(
        "Get 100 random values in store with 10_000 random values",
        |kv| {
            for _ in 0..10_000 {
                kv.set(random_bytes(), random_bytes());
            }
        },
        |kv| {
            for _ in 0..100 {
                black_box(kv.get(&random_bytes()));
            }
        },
    );
}

/*
fn main() {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
    benchmark("Add 10_000 random 20 length values",|| {
        for _ in 0..10_000 {
            kv.set(random_bytes(), random_bytes());
        }
    });
    fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
}
*/
