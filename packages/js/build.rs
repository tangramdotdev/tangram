fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	#[cfg(feature = "v8")]
	self::v8::build();
}

#[cfg(feature = "v8")]
mod v8 {
	use std::path::{Path, PathBuf};

	pub fn build() {
		// Get the out dir path.
		let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

		// Install dependencies.
		println!("cargo:rerun-if-env-changed=NODE_PATH");
		if std::env::var("NODE_PATH").ok().is_none() {
			println!("cargo:rerun-if-changed=../../bun.lock");

			// Acquire an exclusive lock on the node_modules.lock file to ensure only one build script runs bun install at a time.
			let lock_path = Path::new("../../node_modules.lock");
			let lock_file = std::fs::File::create(lock_path).unwrap();
			let lock_file = scopeguard::guard(lock_file, |_| {
				std::fs::remove_file(lock_path).ok();
			});
			lock_file.lock().unwrap();

			std::process::Command::new("bun")
				.args(["install", "--frozen-lockfile"])
				.status()
				.unwrap()
				.success()
				.then_some(())
				.unwrap();
		}

		// Build the js.
		println!("cargo:rerun-if-changed=../../packages/clients/js");
		println!("cargo:rerun-if-changed=./src");
		std::process::Command::new("bun")
			.args(["run", "check"])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
		std::process::Command::new("bunx")
			.args([
				"esbuild",
				"--bundle",
				"--minify",
				&format!("--outdir={}", out_dir_path.display()),
				"--sourcemap=external",
				"./src/main.ts",
			])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
		fixup_source_map(out_dir_path.join("main.js.map"));

		// Initialize V8.
		let platform = v8::new_default_platform(0, false).make_shared();
		v8::V8::initialize_platform(platform);
		v8::V8::initialize();

		// Create the snapshot.
		let path = out_dir_path.join("main.heapsnapshot");
		let snapshot = create_snapshot(out_dir_path.join("main.js"));
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
}
