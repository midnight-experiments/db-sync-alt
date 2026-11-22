//! Offline comparison of finalized provider recordings and restored SQL rows.
#[path = "../../../../crates/dsa-provider/tests/support/corpus.rs"]
mod corpus;
fn main() {
    let mut args = std::env::args().skip(1);
    let first = args.next().expect("corpus directory");
    if first == "--missing-supplements" {
        corpus::run_mode(
            std::path::Path::new(&args.next().expect("corpus directory")),
            true,
        );
    } else {
        corpus::run(std::path::Path::new(&first));
    }
}
