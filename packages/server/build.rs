fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Initialize V8.
	let platform = v8::new_default_platform(0, false).make_shared();
	v8::V8::initialize_platform(platform);
	v8::V8::initialize();

	// Get the out dir path.
	let out_dir_path = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	// Build the language.
	println!("cargo:rerun-if-changed=../../node_modules");
	println!("cargo:rerun-if-changed=../../packages/language");
	let status = std::process::Command::new("npm")
		.args(["run", "-w", "@tangramdotdev/language", "build"])
		.status()
		.unwrap();
	assert!(status.success());

	// Build the runtime.
	println!("cargo:rerun-if-changed=../../node_modules");
	println!("cargo:rerun-if-changed=../../packages/runtime");
	let status = std::process::Command::new("npm")
		.args(["run", "-w", "@tangramdotdev/runtime", "build"])
		.status()
		.unwrap();
	assert!(status.success());

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
