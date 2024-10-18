use sha2::Digest as _;
use std::{
	io::Read as _,
	path::{Path, PathBuf},
};

fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Initialize V8.
	let platform = v8::new_default_platform(0, false).make_shared();
	v8::V8::initialize_platform(platform);
	v8::V8::initialize();

	// Get the out dir path.
	let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	// Get the dash and env binaries.
	let version = "v2024.10.03";
	let binaries = [
		(
			"dash_aarch64_linux",
			"./bin/dash",
			"d1e6ed42b0596507ebfa9ce231e2f42cc67f823cc56c0897c126406004636ce7",
		),
		(
			"dash_x86_64_linux",
			"./bin/dash",
			"d23258e559012dc66cc82d9def66b51e9c41f9fb88f8e9e6a5bd19d231028a64",
		),
		(
			"env_aarch64_linux",
			"./bin/env",
			"b2985354036c4deea9b107f099d853ac2d7c91a095dc285922f6dab72ae1474c",
		),
		(
			"env_x86_64_linux",
			"./bin/env",
			"fceb5be5a7d6f59a026817ebb17be2bcc294d753f1528cbc921eb9015b9ff87b",
		),
	];
	for (name, path, expected) in binaries {
		let env_var = name.to_ascii_uppercase();
		println!("cargo:rerun-if-env-changed={env_var}");

		// Determine the cache path.
		let cache_path = out_dir_path.join(expected);

		// Download if necessary.
		if !cache_path.exists() {
			// If the environment variable is set, use it.
			let bytes = if let Ok(path) = std::env::var(env_var) {
				std::fs::read(&path).unwrap()
			} else {
				// Compute the URL.
				let url = format!("https://github.com/tangramdotdev/bootstrap/releases/download/{version}/{name}.tar.zst");

				// Download.
				reqwest::blocking::get(url).unwrap().bytes().unwrap().into()
			};

			// Verify.
			let mut hasher = sha2::Sha256::new();
			hasher.update(&bytes);
			let actual = hasher.finalize();
			let expected = data_encoding::HEXLOWER.decode(expected.as_bytes()).unwrap();
			assert_eq!(actual.as_slice(), expected);

			// Write to the cache path.
			std::fs::write(&cache_path, &bytes).unwrap();
		}

		// Read.
		let bytes = std::fs::read(&cache_path).unwrap();

		// Decompress.
		let bytes = zstd::decode_all(std::io::Cursor::new(bytes)).unwrap();

		// Unpack.
		let mut read = Vec::new();
		tar::Archive::new(std::io::Cursor::new(bytes))
			.entries()
			.unwrap()
			.map(|entry| entry.unwrap())
			.find(|entry| entry.path().unwrap().as_ref().to_str().unwrap() == path)
			.unwrap()
			.read_to_end(&mut read)
			.unwrap();
		let bytes = read;

		// Write.
		std::fs::write(out_dir_path.join(name), bytes).unwrap();
	}

	// Install dependencies.
	println!("cargo:rerun-if-changed=../../bun.lockb");
	let node_modules_path = out_dir_path.join("node_modules");
	let node_path = if let Ok(node_path) = std::env::var("NODE_PATH") {
		PathBuf::from(node_path)
	} else {
		std::process::Command::new("bun")
			.args(["install", "--frozen-lockfile"])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
		PathBuf::from(std::env::var("CARGO_WORKSPACE_DIR").unwrap()).join("node_modules")
	};
	if node_modules_path.is_symlink() {
		std::fs::remove_file(&node_modules_path).unwrap();
	}
	std::os::unix::fs::symlink(&node_path, &node_modules_path).unwrap();

	// Create the lib path.
	let lib_path = out_dir_path.join("lib");
	std::fs::create_dir_all(&lib_path).unwrap();

	// Copy the tangram.d.ts.
	println!("cargo:rerun-if-changed=../../packages/runtime/tangram.d.ts");
	std::fs::copy(
		"../../packages/runtime/tangram.d.ts",
		lib_path.join("tangram.d.ts"),
	)
	.unwrap();

	// Copy the typescript libraries.
	let paths = glob::glob(
		node_modules_path
			.join("typescript/lib/lib.es*.d.ts")
			.to_str()
			.unwrap(),
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

	// Build the compiler.
	println!("cargo:rerun-if-changed=../../packages/compiler");
	std::process::Command::new("bun")
		.args([
			"build",
			"--minify",
			&format!("--outdir={}", out_dir_path.display()),
			"--sourcemap=external",
			"../../packages/compiler/src/main.ts",
		])
		.status()
		.unwrap()
		.success()
		.then_some(())
		.unwrap();
	std::fs::rename(
		out_dir_path.join("main.js"),
		out_dir_path.join("compiler.js"),
	)
	.unwrap();
	std::fs::rename(
		out_dir_path.join("main.js.map"),
		out_dir_path.join("compiler.js.map"),
	)
	.unwrap();
	fixup_source_map(out_dir_path.join("compiler.js.map"));

	// Build the runtime.
	println!("cargo:rerun-if-changed=../../packages/runtime");
	std::process::Command::new("bun")
		.args([
			"build",
			"--minify",
			&format!("--outdir={}", out_dir_path.display()),
			"--sourcemap=external",
			"../../packages/runtime/src/main.ts",
		])
		.status()
		.unwrap()
		.success()
		.then_some(())
		.unwrap();
	std::fs::rename(
		out_dir_path.join("main.js"),
		out_dir_path.join("runtime.js"),
	)
	.unwrap();
	std::fs::rename(
		out_dir_path.join("main.js.map"),
		out_dir_path.join("runtime.js.map"),
	)
	.unwrap();
	fixup_source_map(out_dir_path.join("runtime.js.map"));

	// Create the compiler snapshot.
	let path = out_dir_path.join("compiler.heapsnapshot");
	let snapshot = create_snapshot(out_dir_path.join("compiler.js"));
	std::fs::write(path, snapshot).unwrap();

	// Create the runtime snapshot.
	let path = out_dir_path.join("runtime.heapsnapshot");
	let snapshot = create_snapshot(out_dir_path.join("runtime.js"));
	std::fs::write(path, snapshot).unwrap();
}

fn create_snapshot(path: impl AsRef<Path>) -> v8::StartupData {
	// Create the isolate.
	let mut isolate = v8::Isolate::snapshot_creator(None, None);

	{
		// Create a context.
		let handle_scope = &mut v8::HandleScope::new(&mut isolate);
		let context = v8::Context::new(handle_scope, v8::ContextOptions::default());
		handle_scope.set_default_context(context);
		let scope = &mut v8::ContextScope::new(handle_scope, context);

		// Compile the script.
		let script = std::fs::read_to_string(path).unwrap();
		let script = v8::String::new(scope, &script).unwrap();
		let resource_name = v8::Integer::new(scope, 0).into();
		let resource_line_offset = 0;
		let resource_column_offset = 0;
		let resource_is_shared_cross_origin = false;
		let script_id = 0;
		let source_map_url = None;
		let resource_is_opaque = true;
		let is_wasm = false;
		let is_module = false;
		let host_defined_options = None;
		let origin = v8::ScriptOrigin::new(
			scope,
			resource_name,
			resource_line_offset,
			resource_column_offset,
			resource_is_shared_cross_origin,
			script_id,
			source_map_url,
			resource_is_opaque,
			is_wasm,
			is_module,
			host_defined_options,
		);
		let script = v8::Script::compile(scope, script, Some(&origin)).unwrap();

		// Run the script.
		script.run(scope).unwrap();
	}

	// Create the snapshot.
	isolate.create_blob(v8::FunctionCodeHandling::Keep).unwrap()
}

fn fixup_source_map(path: impl AsRef<Path>) {
	let bytes = std::fs::read(&path).unwrap();
	let mut json = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
	let sources = json.get_mut("sources").unwrap().as_array_mut().unwrap();
	for source in sources.iter_mut() {
		let serde_json::Value::String(source) = source else {
			panic!();
		};
		let prefix = "../";
		while source.strip_prefix(prefix).is_some() {
			source.drain(..prefix.len()).for_each(drop);
		}
	}
	let bytes = serde_json::to_vec(&json).unwrap();
	std::fs::write(&path, bytes).unwrap();
}
