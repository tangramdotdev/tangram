use std::path::PathBuf;

fn main() {
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=FDB_LIB_PATH");

	if std::env::var_os("CARGO_FEATURE_FOUNDATIONDB").is_none() {
		return;
	}

	if std::env::var("CARGO_CFG_TARGET_OS").unwrap() != "macos" {
		return;
	}

	let lib_path = resolve_fdb_lib_path();
	println!(
		"cargo:rustc-link-arg-bin=tangram=-Wl,-rpath,{}",
		lib_path.display()
	);
}

fn resolve_fdb_lib_path() -> PathBuf {
	if let Ok(path) = std::env::var("FDB_LIB_PATH") {
		return PathBuf::from(path);
	}
	if let Ok(out_dir) = std::env::var("DEP_TANGRAM_FDB_OUT_DIR") {
		let out_dir = PathBuf::from(out_dir);
		let candidates = [out_dir.join("fdb/usr/local/lib"), out_dir.join("fdb")];
		for path in candidates {
			if path.join("libfdb_c.dylib").exists() {
				return path;
			}
		}
	}
	PathBuf::from("/usr/local/lib")
}
