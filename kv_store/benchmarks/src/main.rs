extern crate rand;

use std::fs;
use std::mem::forget;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::thread_rng;

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

fn random_entries(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n).map(|_| (random_bytes(), random_bytes())).collect()
}

fn shuffle_vec<T>(vec: &mut Vec<T>) {
    vec.shuffle(&mut thread_rng());
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
    mut setup_f: impl FnMut(&mut kv_store::KVStore) -> (),
    mut f: impl FnMut(&mut kv_store::KVStore) -> (),
) {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
    setup_f(&mut kv);
    benchmark(name, move || {
        f(&mut kv);
    });
    fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
}

fn serialize_duration(d: Duration) -> Vec<u8> {
    d.as_micros().to_be_bytes().to_vec()
}

fn deserialize_duration(bytes: &[u8]) -> u128 {
    assert!(bytes.len() == 16);
    let mut arr: [u8; 16] = Default::default();
    arr.copy_from_slice(&bytes[0..16]);
    u128::from_be_bytes(arr)
}

fn main() {
    let mut kv: kv_store::KVStore = kv_store::KVStore::new("./tmp-bench-results");

    benchmark_kv_store("Add 10_000 random entries to empty store", |kv| {
        for _ in 0..10_000 {
            kv.set(random_bytes(), random_bytes());
        }
    });

    benchmark_kv_store("Get 10_000 random entries in empty store", |kv| {
        for _ in 0..10_000 {
            black_box(kv.get(&random_bytes()));
        }
    });

    benchmark_kv_store_with_setup(
        "Get 100 random entries in store with 100 random entries",
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
        "Get 100 random entries in store with 1000 random entries",
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
        "Get 100 random entries in store with 10_000 random entries",
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

    {
        let entries = random_entries(100);
        benchmark_kv_store_with_setup(
            "Get 100 known entries in random order in store with 100 random entries",
            |kv| {
                let add_entries = entries.clone();
                for entry in add_entries {
                    kv.set(entry.0, entry.1);
                }
            },
            |kv| {
                let mut get_entries = entries.clone();
                shuffle_vec(&mut get_entries);
                for entry in get_entries {
                    black_box(kv.get(&entry.0));
                }
            },
        );
    }

    {
        let some_entries = random_entries(100);
        let mut all_entries = random_entries(900);
        all_entries.extend(some_entries.iter().cloned());

        benchmark_kv_store_with_setup(
            "Get 100 known entries in random order in store with 1000 random entries",
            |kv| {
                let add_entries = all_entries.clone();
                for entry in add_entries {
                    kv.set(entry.0, entry.1);
                }
            },
            |kv| {
                let mut get_entries = some_entries.clone();
                shuffle_vec(&mut get_entries);
                for entry in get_entries {
                    black_box(kv.get(&entry.0));
                }
            },
        );
    }

    {
        let some_entries = random_entries(100);
        let mut all_entries = random_entries(9900);
        all_entries.extend(some_entries.iter().cloned());

        benchmark_kv_store_with_setup(
            "Get 100 known entries in random order in store with 10_000 random entries",
            |kv| {
                let add_entries = all_entries.clone();
                for entry in add_entries {
                    kv.set(entry.0, entry.1);
                }
            },
            |kv| {
                let mut get_entries = some_entries.clone();
                shuffle_vec(&mut get_entries);
                for entry in get_entries {
                    black_box(kv.get(&entry.0));
                }
            },
        );
    }

    {
        let some_entries = random_entries(100);
        let mut all_entries = random_entries(99900);
        all_entries.extend(some_entries.iter().cloned());

        benchmark_kv_store_with_setup(
            "Get 100 known entries in random order in store with 100_000 random entries",
            |kv| {
                let add_entries = all_entries.clone();
                for entry in add_entries {
                    kv.set(entry.0, entry.1);
                }
            },
            |kv| {
                let mut get_entries = some_entries.clone();
                shuffle_vec(&mut get_entries);
                for entry in get_entries {
                    black_box(kv.get(&entry.0));
                }
            },
        );
    }
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
