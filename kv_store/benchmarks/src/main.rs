extern crate rand;

use std::fs;
use std::mem::forget;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::thread_rng;

const SAMPLES: u32 = 32;
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

fn print_benchmark_result(
    benchmark_results: &mut kv_store::KVStore,
    name: &str,
    duration: Duration,
) {
    match benchmark_results.get(&serialize_string(name)) {
        Some(previous_duration_micros) => {
            let previous_duration = deserialize_duration(&previous_duration_micros);
            println!(
                "{}: {:?} (previous: {:?})",
                name, duration, previous_duration
            );
        }
        None => {
            println!("{}: {:?}", name, duration);
        }
    };

    benchmark_results.set(serialize_string(name), serialize_duration(duration));
}

fn benchmark(mut f: impl FnMut()) -> Duration {
    thread::sleep(Duration::from_millis(10));
    let start = Instant::now();
    f();
    let duration = start.elapsed();
    thread::sleep(Duration::from_millis(10));
    duration
}

fn benchmark_kv_store(
    mut benchmark_results: &mut kv_store::KVStore,
    name: &str,
    mut setup_f: impl FnMut(&mut kv_store::KVStore) -> (),
    mut f: impl FnMut(&mut kv_store::KVStore) -> (),
) {
    let mut duration = Duration::new(0,0);
    for _ in 0..SAMPLES {
        let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
        setup_f(&mut kv);
        let d = benchmark(|| {
            f(&mut kv);
        });
        duration += d;
        std::mem::drop(kv);
        fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
    }
    print_benchmark_result(&mut benchmark_results, name, duration / SAMPLES);
}

fn serialize_duration(d: Duration) -> Vec<u8> {
    d.as_micros().to_be_bytes().to_vec()
}

fn deserialize_duration(bytes: &[u8]) -> Duration {
    assert!(bytes.len() == 16);
    let mut arr: [u8; 16] = Default::default();
    arr.copy_from_slice(&bytes[0..16]);
    Duration::from_micros(u128::from_be_bytes(arr) as u64)
}

fn serialize_string(s: &str) -> Vec<u8> {
    String::from(s).into_bytes()
}

/*
fn deserialize_string(bytes: &[u8]) -> &str {
    std::str::from_utf8(bytes).unwrap()
}
*/

fn main() {
    let _ = fs::remove_dir_all(TMP_DIR);
    let mut benchmark_results: kv_store::KVStore = kv_store::KVStore::new("./previous_benchmarks");

    benchmark_kv_store(
        &mut benchmark_results,
        "Add 10_000 random entries to empty store",
        |_| {},
        |kv| {
            for _ in 0..10_000 {
                kv.set(random_bytes(), random_bytes());
            }
        },
    );

    benchmark_kv_store(
        &mut benchmark_results,
        "Get 10_000 random entries in empty store",
        |_| {},
        |kv| {
            for _ in 0..10_000 {
                black_box(kv.get(&random_bytes()));
            }
        },
    );

    benchmark_kv_store(
        &mut benchmark_results,
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

    benchmark_kv_store(
        &mut benchmark_results,
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

    benchmark_kv_store(
        &mut benchmark_results,
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
        benchmark_kv_store(
            &mut benchmark_results,
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

        benchmark_kv_store(
            &mut benchmark_results,
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

        benchmark_kv_store(
            &mut benchmark_results,
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

        benchmark_kv_store(
            &mut benchmark_results,
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
