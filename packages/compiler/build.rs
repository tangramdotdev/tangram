fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	// Build the library.
	let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
	std::fs::create_dir_all(output.join("lib")).unwrap();
	#[cfg(feature = "javascript")]
	self::library::build();
	#[cfg(feature = "ty")]
	self::python::build();
}

#[cfg(feature = "javascript")]
mod library {
	use {
		serde_json::json,
		std::path::{Path, PathBuf},
	};

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

		// Create the lib path.
		let lib_path = out_dir_path.join("lib");
		std::fs::create_dir_all(&lib_path).unwrap();

		// Generate the Tangram declarations.
		println!("cargo:rerun-if-changed=../../packages/javascript/src/tangram.d.ts");
		println!("cargo:rerun-if-changed=../../packages/clients/javascript/package.json");
		println!("cargo:rerun-if-changed=../../packages/clients/javascript/tsconfig.json");
		println!("cargo:rerun-if-changed=../../packages/clients/javascript/src");
		let tangram_declarations_path = lib_path.join("tangram");
		if tangram_declarations_path.exists() {
			std::fs::remove_dir_all(&tangram_declarations_path).unwrap();
		}
		let manifest_directory_path = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
		let client_path = std::fs::canonicalize(
			manifest_directory_path.join("../../packages/clients/javascript"),
		)
		.unwrap();
		let mut compiler_options = json!({
			"declaration": true,
			"emitDeclarationOnly": true,
			"noEmit": false,
			"outDir": tangram_declarations_path,
			"rootDir": client_path.join("src"),
		});
		if let Ok(node_path) = std::env::var("NODE_PATH") {
			let compiler_options = compiler_options.as_object_mut().unwrap();
			compiler_options.insert(
				"paths".into(),
				json!({
					"*": [
						format!("{node_path}/*"),
						format!("{}/*", client_path.join("node_modules").display()),
					],
				}),
			);
			// The client's tsconfig resolves type roots relative to itself, which has no node_modules under NODE_PATH.
			compiler_options.insert("typeRoots".into(), json!([format!("{node_path}/@types")]));
		}
		let tsconfig_path = out_dir_path.join("tangram.tsconfig.json");
		let tsconfig = json!({
			"extends": client_path.join("tsconfig.json"),
			"compilerOptions": compiler_options,
			"include": [client_path.join("src/**/*")],
		});
		std::fs::write(
			&tsconfig_path,
			serde_json::to_string_pretty(&tsconfig).unwrap(),
		)
		.unwrap();
		std::process::Command::new("bunx")
			.args(["tsc", "--project"])
			.arg(&tsconfig_path)
			.status()
			.unwrap()
			.success()
			.then_some(())
			.unwrap();
		let declarations = std::fs::read_to_string("../../packages/javascript/src/tangram.d.ts")
			.unwrap()
			.replace(r#""@tangramdotdev/client""#, r#""./tangram/index.ts""#);
		std::fs::write(lib_path.join("tangram.d.ts"), declarations).unwrap();

		// Copy the typescript libraries.
		let node_modules_path = match std::env::var("NODE_PATH") {
			Ok(path) => PathBuf::from(path),
			Err(_) => PathBuf::from("../../node_modules"),
		};
		// The libraries are in the typescript package's platform-specific sibling package, which bun's isolated and hoisted linkers place differently.
		let typescript_path = std::fs::canonicalize(node_modules_path.join("typescript"))
			.expect("failed to find the typescript package");
		let patterns = [
			typescript_path.join("../@typescript/typescript-*/lib/lib.es5.d.ts"),
			node_modules_path.join("@typescript/typescript-*/lib/lib.es5.d.ts"),
		];
		let typescript_lib_path = patterns
			.iter()
			.flat_map(|pattern| glob::glob(&pattern.to_string_lossy()).unwrap())
			.find_map(Result::ok)
			.and_then(|path| path.parent().map(Path::to_owned))
			.expect("failed to find the typescript libraries");
		let paths =
			glob::glob(&typescript_lib_path.join("lib.es*.d.ts").to_string_lossy()).unwrap();
		for path in paths {
			let path = path.unwrap();
			copy(&path, &lib_path.join(path.file_name().unwrap()));
		}
		copy(
			&typescript_lib_path.join("lib.decorators.d.ts"),
			&lib_path.join("lib.decorators.d.ts"),
		);
		copy(
			&typescript_lib_path.join("lib.decorators.legacy.d.ts"),
			&lib_path.join("lib.decorators.legacy.d.ts"),
		);
	}

	// Remove the destination first, because `std::fs::copy` preserves the source's permissions and the libraries may be read only.
	fn copy(from: &Path, to: &Path) {
		std::fs::remove_file(to).ok();
		std::fs::copy(from, to).unwrap();
	}
}

#[cfg(feature = "ty")]
mod python {
	use std::{path::PathBuf, process::Command};

	pub fn build() {
		for path in [
			"../python/build/library.py",
			"../clients/python/pyproject.toml",
			"../../uv.lock",
		] {
			println!("cargo:rerun-if-changed={path}");
		}
		let manifest_directory = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
		tangram_python_build::rerun_if_client_changed(
			&manifest_directory.join("../clients/python/src"),
		);
		let output = PathBuf::from(std::env::var_os("OUT_DIR").unwrap()).join("python");
		if output.exists() {
			std::fs::remove_dir_all(&output).unwrap();
		}
		std::fs::create_dir_all(&output).unwrap();
		println!("cargo:rerun-if-env-changed=TANGRAM_PYTHON_HOST_DISTRIBUTION");
		let build = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
		let workspace =
			PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("../..");
		let host = std::env::var("HOST").unwrap();
		let python = tangram_python_build::host(&build, &host);
		let packages = tangram_python_build::packages(&python, &workspace, &build);
		let status = Command::new(python)
			.arg("-I")
			.arg("../python/build/library.py")
			.arg(&output)
			.arg(&packages)
			.arg(workspace.join("packages/clients/python/src"))
			.status()
			.unwrap();
		assert!(
			status.success(),
			"failed to prepare the python client library"
		);
	}
}
