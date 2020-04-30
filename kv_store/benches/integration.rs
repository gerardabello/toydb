extern crate rand;

use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use criterion::{black_box, criterion_group, criterion_main};

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;

use kv_store;

fn random_string() -> String {
    thread_rng().sample_iter(&Alphanumeric).take(30).collect()
}

fn add_value(c: &mut Criterion) {
    let mut group = c.benchmark_group("add value in filled store");
    for size in [1000, 10000, 100000, 1000000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut kv: kv_store::KVStore = Default::default();
            for _ in 0..size {
                kv.set(random_string(), black_box(random_string()));
            }
            b.iter(move || { 
                kv.set(random_string(), black_box(random_string()));
                kv.get("a")
            })
        });
    }
    group.finish();
}

criterion_group!(benches, add_value);
criterion_main!(benches);

/*
#![feature(test)]

extern crate test;
use test::Bencher;



#[bench]
fn replacing_same_value(b: &mut Bencher) {
    b.iter(|| {
        let mut kv : kv_store::KVStore = Default::default();
        (0..BENCH_SIZE).map(|_| {
            kv.set("a", "mandarina");
        });

        kv.get("a")
    })
}

*/
