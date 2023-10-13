use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    build_stamp::write_stamp_file_at_compile_time(&repo, &out_dir);
}
