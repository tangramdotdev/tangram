use std::path::PathBuf;

fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Get the out dir path.
	let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	// Install dependencies.
	println!("cargo:rerun-if-env-changed=NODE_PATH");
	if std::env::var("NODE_PATH").ok().is_none() {
		println!("cargo:rerun-if-changed=../../bun.lock");
		std::process::Command::new("bun")
			.args(["install", "--frozen-lockfile"])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
	}

	// Create the lib path.
	let lib_path = out_dir_path.join("lib");
	std::fs::create_dir_all(&lib_path).unwrap();

	// Copy the tangram.d.ts.
	println!("cargo:rerun-if-changed=../../packages/js/src/tangram.d.ts");
	std::fs::copy(
		"../../packages/js/src/tangram.d.ts",
		lib_path.join("tangram.d.ts"),
	)
	.unwrap();

	// Copy the typescript libraries.
	let node_modules_path = match std::env::var("NODE_PATH") {
		Ok(path) => PathBuf::from(path),
		Err(_) => PathBuf::from("../../node_modules"),
	};
	let paths = glob::glob(
		&node_modules_path
			.join("typescript/lib/lib.es*.d.ts")
			.to_string_lossy(),
	)
	.unwrap();
	for path in paths {
		let path = path.unwrap();
		std::fs::copy(&path, lib_path.join(path.file_name().unwrap())).unwrap();
	}
	std::fs::copy(
		node_modules_path.join("typescript/lib/lib.decorators.d.ts"),
		lib_path.join("lib.decorators.d.ts"),
	)
	.unwrap();
	std::fs::copy(
		node_modules_path.join("typescript/lib/lib.decorators.legacy.d.ts"),
		lib_path.join("lib.decorators.legacy.d.ts"),
	)
	.unwrap();
}
