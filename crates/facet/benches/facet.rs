//! `cargo bench -p facet-db`. Short runs: a number to compare across
//! commits, not a precise one. Filled in with the engine's batch paths as
//! they land.

use criterion::{Criterion, criterion_group, criterion_main};

fn keyenc(c: &mut Criterion) {
    use facet::KeyType;
    c.bench_function("keyenc/tuple", |b| {
        let k = (uuid::Uuid::from_u128(7), String::from("order-1234"), 42i64);
        b.iter(|| std::hint::black_box(k.to_key()));
    });
}

criterion_group!(benches, keyenc);
criterion_main!(benches);
