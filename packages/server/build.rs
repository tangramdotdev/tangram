use sha2::Digest as _;
use std::io::Read as _;

fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Initialize V8.
	let platform = v8::new_default_platform(0, false).make_shared();
	v8::V8::initialize_platform(platform);
	v8::V8::initialize();

	// Get the out dir path.
	let out_dir_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	// Get the dash and env binaries.
	let version = "v2024.04.02";
	let binaries = [
		(
			"dash_aarch64_linux",
			"./bin/dash",
			"89a1cab57834f81cdb188d5f40b2e98aaff2a5bdae4e8a5d74ad0b2a7672d36b",
		),
		(
			"dash_x86_64_linux",
			"./bin/dash",
			"899adb46ccf4cddc7bfeb7e83a6b2953124035c350d6f00f339365e3b01b920e",
		),
		(
			"env_aarch64_linux",
			"./bin/env",
			"da4fed85cc4536de95b32f5a445e169381ca438e76decdbb4f117a1d115b0184",
		),
		(
			"env_x86_64_linux",
			"./bin/env",
			"ea7b6f8ffa359519660847780a61665bb66748aee432dec8a35efb0855217b95",
		),
	];
	for (name, path, expected) in binaries {
		// Determine the cache path.
		let cache_path = out_dir_path.join(expected);

		// Download if necessary.
		if !cache_path.exists() {
			// Compute the URL.
			let url = format!("https://github.com/tangramdotdev/bootstrap/releases/download/{version}/{name}.tar.zst");

			// Download.
			let bytes = reqwest::blocking::get(url).unwrap().bytes().unwrap();

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
	println!("cargo:rerun-if-changed=../../node_modules/typescript/lib");
	let paths = glob::glob("../../node_modules/typescript/lib/lib.es*.d.ts").unwrap();
	for path in paths {
		let path = path.unwrap();
		std::fs::copy(&path, lib_path.join(path.file_name().unwrap())).unwrap();
	}
	std::fs::copy(
		"../../node_modules/typescript/lib/lib.decorators.d.ts",
		lib_path.join("lib.decorators.d.ts"),
	)
	.unwrap();
	std::fs::copy(
		"../../node_modules/typescript/lib/lib.decorators.legacy.d.ts",
		lib_path.join("lib.decorators.legacy.d.ts"),
	)
	.unwrap();

	// Build the compiler.
	println!("cargo:rerun-if-changed=../../node_modules");
	println!("cargo:rerun-if-changed=../../packages/compiler");
	std::process::Command::new("bunx")
		.args([
			"esbuild",
			"--bundle",
			"--entry-names=compiler",
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
	fixup_source_map(out_dir_path.join("compiler.js.map"));

	// Build the runtime.
	println!("cargo:rerun-if-changed=../../node_modules");
	println!("cargo:rerun-if-changed=../../packages/runtime");
	std::process::Command::new("bunx")
		.args([
			"esbuild",
			"--bundle",
			"--entry-names=runtime",
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

fn create_snapshot(path: impl AsRef<std::path::Path>) -> v8::StartupData {
	// Create the isolate.
	let mut isolate = v8::Isolate::snapshot_creator(None);

	{
		// Create a context.
		let handle_scope = &mut v8::HandleScope::new(&mut isolate);
		let context = v8::Context::new(handle_scope);
		handle_scope.set_default_context(context);
		let scope = &mut v8::ContextScope::new(handle_scope, context);

		// Compile the script.
		let script = std::fs::read_to_string(path).unwrap();
		let script = v8::String::new(scope, &script).unwrap();
		let resource_name =
			v8::String::new_external_onebyte_static(scope, "[global]".as_bytes()).unwrap();
		let resource_line_offset = 0;
		let resource_column_offset = 0;
		let resource_is_shared_cross_origin = false;
		let script_id = 0;
		let source_map_url = v8::undefined(scope).into();
		let resource_is_opaque = true;
		let is_wasm = false;
		let is_module = false;
		let origin = v8::ScriptOrigin::new(
			scope,
			resource_name.into(),
			resource_line_offset,
			resource_column_offset,
			resource_is_shared_cross_origin,
			script_id,
			source_map_url,
			resource_is_opaque,
			is_wasm,
			is_module,
		);
		let script = v8::Script::compile(scope, script, Some(&origin)).unwrap();

		// Run the script.
		script.run(scope).unwrap();
	}

	// Create the snapshot.
	isolate.create_blob(v8::FunctionCodeHandling::Keep).unwrap()
}

fn fixup_source_map(path: impl AsRef<std::path::Path>) {
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
