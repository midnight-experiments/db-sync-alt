//! Offline comparison of finalized provider recordings and restored SQL rows.
#[path = "support/corpus.rs"]
mod corpus;
fn main() {
    let path = std::env::args().nth(1).expect("corpus directory");
    corpus::run(std::path::Path::new(&path));
}
