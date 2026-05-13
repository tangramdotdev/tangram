use std::{env, path::PathBuf};

fn main() {
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=FDB_LIB_PATH");

	if env::var_os("CARGO_FEATURE_FOUNDATIONDB").is_none() {
		return;
	}

	if env::var("CARGO_CFG_TARGET_OS").unwrap() != "macos" {
		return;
	}

	let lib_path =
		env::var("FDB_LIB_PATH").map_or_else(|_| PathBuf::from("/usr/local/lib"), PathBuf::from);
	println!(
		"cargo:rustc-link-arg-bin=tangram=-Wl,-rpath,{}",
		lib_path.display()
	);
}
