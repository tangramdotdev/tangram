fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Initialize V8.
	let platform = v8::new_default_platform(0, false).make_shared();
	v8::V8::initialize_platform(platform);
	v8::V8::initialize();

	// Get the out dir path.
	let out_dir_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	// Create the lib path.
	let lib_path = out_dir_path.join("lib");
	std::fs::create_dir_all(&lib_path).unwrap();

	// Copy the tangram.d.ts.
	println!("cargo:rerun-if-changed=src/language/tangram.d.ts");
	std::fs::copy("src/language/tangram.d.ts", lib_path.join("tangram.d.ts")).unwrap();

	// Copy the typescript libraries.
	println!("cargo:rerun-if-changed=../../node_modules");
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

	// Build the language.
	println!("cargo:rerun-if-changed=../../node_modules");
	println!("cargo:rerun-if-changed=../../packages/language");
	std::process::Command::new("bunx")
		.args([
			"esbuild",
			"--bundle",
			"--entry-names=language",
			"--minify",
			&format!("--outdir={}", out_dir_path.display()),
			"--sourcemap=external",
			"../../packages/language/src/main.ts",
		])
		.status()
		.unwrap()
		.success()
		.then_some(())
		.unwrap();
	fixup_source_map(out_dir_path.join("language.js.map"));

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

	// Create the language snapshot.
	let path = out_dir_path.join("language.heapsnapshot");
	let snapshot = create_snapshot(out_dir_path.join("language.js"));
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
