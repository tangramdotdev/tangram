use {
	indoc::formatdoc,
	std::path::{Path, PathBuf},
};

fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	js();

	#[cfg(feature = "quickjs")]
	self::quickjs::bytecode();

	#[cfg(feature = "v8")]
	self::v8::snapshot();
}

fn js() {
	// Get the out dir path.
	let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
	let manifest_directory_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
	let client_path = manifest_directory_path.join("../clients/js");

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

	// Build the client.
	println!("cargo:rerun-if-changed=../../packages/clients/js/package.json");
	println!("cargo:rerun-if-changed=../../packages/clients/js/build.ts");
	println!("cargo:rerun-if-changed=../../packages/clients/js/src");
	println!("cargo:rerun-if-changed=../../packages/clients/js/tsconfig.json");
	println!("cargo:rerun-if-changed=../../packages/clients/js/wasm");
	std::process::Command::new("bun")
		.args(["run", "build"])
		.current_dir(&client_path)
		.status()
		.unwrap()
		.success()
		.then_some(())
		.unwrap();

	// Build the js.
	println!("cargo:rerun-if-changed=./src");
	let node_path = std::env::var("NODE_PATH").ok();
	if let Some(node_path) = &node_path {
		std::fs::write(
			out_dir_path.join("tsconfig.json"),
			formatdoc!(
				r#"
					{{
						"extends": "{manifest_directory_path}/tsconfig.json",
						"compilerOptions": {{
							"paths": {{
								"@tangramdotdev/client": ["{client_path}/dist/index.d.ts"],
								"*": ["{node_path}/*", "{node_path}/../packages/clients/js/node_modules/*"]
							}}
						}},
						"include": ["{manifest_directory_path}/src/**/*"]
					}}
				"#,
				client_path = client_path.display(),
				manifest_directory_path = manifest_directory_path.display(),
			),
		)
		.unwrap();
		std::process::Command::new("bunx")
			.args(["tsc", "--project", out_dir_path.to_str().unwrap()])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
	} else {
		std::process::Command::new("bun")
			.args(["run", "check"])
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
	}
	let alias = format!(
		"--alias:@tangramdotdev/client={}",
		client_path.join("dist/index.js").display()
	);
	let outdir = format!("--outdir={}", out_dir_path.display());
	let mut esbuild = std::process::Command::new("bunx");
	esbuild.args([
		"esbuild",
		&alias,
		"--bundle",
		"--define:process=undefined",
		"--minify",
		&outdir,
		"--sourcemap=external",
		"./src/main.ts",
	]);
	if let Some(node_path) = &node_path {
		esbuild.env(
			"NODE_PATH",
			format!("{node_path}:{node_path}/../packages/js/node_modules:{node_path}/../packages/clients/js/node_modules"),
		);
	}
	esbuild.status().unwrap().success().then_some(()).unwrap();
	fixup_source_map(out_dir_path.join("main.js.map"));
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

#[cfg(feature = "quickjs")]
mod quickjs {
	use {rquickjs as qjs, std::path::PathBuf};

	pub fn bytecode() {
		let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

		// Read the main.js.
		let js_path = out_dir_path.join("main.js");
		let js_source = std::fs::read_to_string(&js_path).unwrap();

		// Create a qjs runtime.
		let rt = qjs::Runtime::new().unwrap();
		let ctx = qjs::Context::full(&rt).unwrap();

		ctx.with(|ctx| {
			// Compile the script to bytecode.
			let module = qjs::Module::declare(ctx, "main", js_source).unwrap();
			let bytecode = module.write(qjs::WriteOptions::default()).unwrap();

			// Write the bytecode.
			let bytecode_path = out_dir_path.join("main.bytecode");
			std::fs::write(&bytecode_path, bytecode).unwrap();
		});
	}
}

#[cfg(feature = "v8")]
mod v8 {
	use std::path::PathBuf;

	pub fn snapshot() {
		let out_dir_path = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

		// Initialize V8.
		let platform = v8::new_default_platform(0, false).make_shared();
		v8::V8::initialize_platform(platform);
		v8::V8::set_flags_from_string("--no-extensible_ro_snapshot");
		v8::V8::initialize();

		// Create the isolate.
		let mut isolate = v8::Isolate::snapshot_creator(None, None);

		{
			// Create a context.
			v8::scope!(scope, &mut isolate);
			let context = v8::Context::new(scope, v8::ContextOptions::default());
			let scope = &mut v8::ContextScope::new(scope, context);

			// Compile the script.
			let path = out_dir_path.join("main.js");
			let script_text = std::fs::read_to_string(&path).unwrap();
			let script_str = v8::String::new(scope, &script_text).unwrap();
			let resource_name = v8::String::new(scope, "main").unwrap().into();
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
			let script = v8::Script::compile(scope, script_str, Some(&origin)).unwrap();

			// Run the script.
			script.run(scope).unwrap();

			// Set the default context for the snapshot.
			scope.set_default_context(context);
		}

		// Create the snapshot.
		let snapshot = isolate.create_blob(v8::FunctionCodeHandling::Keep).unwrap();

		// Write the snapshot.
		let path = out_dir_path.join("main.heapsnapshot");
		std::fs::write(&path, &*snapshot).unwrap();
	}
}
