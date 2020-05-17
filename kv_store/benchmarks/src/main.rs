extern crate rand;
mod utils;

use utils::{
    benchmark_kv_store, benchmark_random_operations, black_box, random_bytes, random_entries,
    shuffle_vec,
};

fn main() {
    let mut benchmark_results: kv_store::KVStore = kv_store::KVStore::new("./previous_benchmarks");

    benchmark_kv_store(
        &mut benchmark_results,
        "Add 100_000 random entries to empty store",
        32,
        |_| {},
        |kv| {
            for _ in 0..100_000 {
                kv.set(random_bytes(), random_bytes());
            }
        },
    );


    benchmark_kv_store(
        &mut benchmark_results,
        "Get 10_000 random entries in empty store",
        32,
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
        32,
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
        32,
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
        8,
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
            32,
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
            32,
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
            32,
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
            32,
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
        let initial = random_entries(100_000);
        let mut get_entries = (&initial[..6_00]).to_vec();
        let set_entries = random_entries(3_00);
        get_entries.append(&mut random_entries(1_00));

        benchmark_random_operations(
            "Store with 100_000 elements. 1000 operations in random order (30% set, 60% get, 10% get missing)",
            &mut benchmark_results,
            initial,
            get_entries,
            set_entries,
        )
    }
}
