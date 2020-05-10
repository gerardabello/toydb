extern crate rand;

use rand::Rng;

use criterion::{black_box, criterion_group, criterion_main};

use criterion::BenchmarkId;
use criterion::Criterion;

use kv_store;

fn random_bytes() -> Vec<u8> {
    (0..20).map(|_| rand::random::<u8>()).collect()
}

fn add_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("add value in filled store");
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut kv: kv_store::KVStore = kv_store::KVStore::new();
            for _ in 0..size {
                kv.set(random_bytes(), black_box(random_bytes()));
            }
            b.iter(|| {
                let key = random_bytes();
                let value = random_bytes();
                kv.set(black_box(key), black_box(value));
            })
        });
    }
    group.finish();
}

fn get_value(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let mut group = c.benchmark_group("get value in filled store");
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut kv: kv_store::KVStore = Default::default();

            // Get a random key from the added ones to search later
            let search_key_index = rng.gen_range(0, size);
            let mut search_key: Vec<u8> = vec![];

            for i in 0..size {
                let key = random_bytes();
                let value = random_bytes();
                if search_key_index == i {
                    search_key = key.to_vec();
                }
                kv.set(key, value);
            }
            b.iter(|| {
                kv.get(black_box(&search_key));
            })
        });
    }
    group.finish();
}

fn get_missing_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("get missing value in filled store");
    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut kv: kv_store::KVStore = Default::default();

            for _ in 0..size {
                kv.set(random_bytes(), black_box(random_bytes()));
            }

            let search_key = random_bytes();
            b.iter(|| {
                kv.get(black_box(&search_key));
            })
        });
    }
    group.finish();
}

criterion_group!(benches, add_value, get_value, get_missing_value);

criterion_main!(benches);
