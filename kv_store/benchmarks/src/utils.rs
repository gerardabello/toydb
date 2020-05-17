extern crate rand;

use std::fs;
use std::mem::forget;
use std::ptr;
use std::thread;
use std::time::{Duration, Instant};

use rand::seq::SliceRandom;
use rand::thread_rng;

const TMP_DIR: &str = "./tmp-dir";

pub fn black_box<T>(dummy: T) -> T {
    unsafe {
        let ret = ptr::read_volatile(&dummy);
        forget(dummy);
        ret
    }
}

pub fn random_bytes() -> Vec<u8> {
    (0..20).map(|_| rand::random::<u8>()).collect()
}

pub fn random_entries(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n).map(|_| (random_bytes(), random_bytes())).collect()
}

pub fn shuffle_vec<T>(vec: &mut Vec<T>) {
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

pub fn benchmark_kv_store(
    mut benchmark_results: &mut kv_store::KVStore,
    name: &str,
    samples: u32,
    mut setup_f: impl FnMut(&mut kv_store::KVStore) -> (),
    mut f: impl FnMut(&mut kv_store::KVStore) -> (),
) {
    let mut duration = Duration::new(0, 0);
    for _ in 0..samples {
        let mut kv: kv_store::KVStore = kv_store::KVStore::new(TMP_DIR);
        setup_f(&mut kv);
        let d = benchmark(|| {
            f(&mut kv);
        });
        duration += d;
        std::mem::drop(kv);
        fs::remove_dir_all(TMP_DIR).expect("Remove tmp folder");
    }
    print_benchmark_result(&mut benchmark_results, name, duration / samples);
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

enum Operation {
    Read((Vec<u8>, Vec<u8>)),
    Write((Vec<u8>, Vec<u8>)),
}

pub fn benchmark_random_operations(
    name: &str,
    mut benchmark_results: &mut kv_store::KVStore,
    mut initial: Vec<(Vec<u8>, Vec<u8>)>,
    gets: Vec<(Vec<u8>, Vec<u8>)>,
    sets: Vec<(Vec<u8>, Vec<u8>)>,
) {
    let mut reads: Vec<Operation> = gets
        .iter()
        .map(|entry| Operation::Read(entry.clone()))
        .collect();

    let mut writes: Vec<Operation> = sets
        .iter()
        .map(|entry| Operation::Write(entry.clone()))
        .collect();

    let mut operations = Vec::new();
    operations.append(&mut writes);
    operations.append(&mut reads);

    shuffle_vec(&mut operations);
    shuffle_vec(&mut initial);

    benchmark_kv_store(
        &mut benchmark_results,
        name,
        1,
        |kv| {
            for entry in &initial {
                kv.set(entry.0.clone(), entry.1.clone());
            }
        },
        |kv| {
            for op in &operations {
                match op {
                    Operation::Write(pair) => {
                        kv.set(pair.0.clone(), pair.1.clone());
                    }
                    Operation::Read(pair) => {
                        black_box(kv.get(&pair.0));
                    }
                }
            }
        },
    );
}
