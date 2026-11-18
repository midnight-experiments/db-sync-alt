#[path = "codegen/catalogue.rs"]
mod catalogue;
fn main() {
    let root = std::path::Path::new("../../reference");
    println!("cargo:rerun-if-changed=../../reference");
    println!("cargo:rerun-if-changed=codegen/catalogue.rs");
    let value = catalogue::extract(root);
    let out =
        std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("node-catalogue.json");
    std::fs::write(out, serde_json::to_string_pretty(&value).unwrap()).unwrap();
}
