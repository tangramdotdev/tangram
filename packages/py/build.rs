use {
	serde_json::Value,
	sha2::{Digest as _, Sha256},
	std::{
		collections::BTreeSet,
		fmt::Write as _,
		path::{Path, PathBuf},
		process::Command,
	},
};

fn main() {
	for path in [
		"build.rs",
		"distributions.json",
		"embed.py",
		"freeze.py",
		"llvm.py",
		"native.py",
		"src",
		"../clients/py/src",
		"../clients/py/library.py",
		"../clients/py/pyproject.toml",
		"../../uv.lock",
	] {
		println!("cargo:rerun-if-changed={path}");
	}
	for name in [
		"OBJCOPY",
		"TANGRAM_PYTHON_DISTRIBUTION",
		"TANGRAM_PYTHON_HOST_DISTRIBUTION",
	] {
		println!("cargo:rerun-if-env-changed={name}");
	}
	let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
	let output = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
	let manifest_directory = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap());
	let relative_library = relative_path(&manifest_directory, &output.join("library"));
	println!(
		"cargo:rustc-env=TANGRAM_PYTHON_LIBRARY={}",
		relative_library.display()
	);
	let target = std::env::var("TARGET").unwrap();
	println!(
		"cargo:rerun-if-env-changed=OBJCOPY_{}",
		target.replace('-', "_")
	);
	let host = std::env::var("HOST").unwrap();
	let static_crt = static_crt(&target);
	let manifest: Value = serde_json::from_str(include_str!("distributions.json")).unwrap();
	let key = if target.ends_with("musl") && static_crt {
		format!("{target}+static")
	} else {
		target.clone()
	};
	let distribution = download(&manifest, &key, &output, "TANGRAM_PYTHON_DISTRIBUTION");
	let host_distribution = if target == host
		&& !static_crt
		&& std::env::var_os("TANGRAM_PYTHON_HOST_DISTRIBUTION").is_none()
	{
		distribution.clone()
	} else {
		download(
			&manifest,
			&host,
			&output,
			"TANGRAM_PYTHON_HOST_DISTRIBUTION",
		)
	};
	let metadata: Value =
		serde_json::from_reader(std::fs::File::open(distribution.join("PYTHON.json")).unwrap())
			.unwrap();
	let host_metadata: Value = serde_json::from_reader(
		std::fs::File::open(host_distribution.join("PYTHON.json")).unwrap(),
	)
	.unwrap();
	assert_eq!(metadata["python_version"], manifest["python"]);
	assert_eq!(metadata["target_triple"].as_str().unwrap(), target);
	assert_eq!(host_metadata["python_version"], manifest["python"]);
	assert_eq!(host_metadata["target_triple"].as_str().unwrap(), host);
	if target.ends_with("musl") {
		let link_mode = if static_crt { "static" } else { "shared" };
		assert_eq!(metadata["libpython_link_mode"].as_str().unwrap(), link_mode);
	}
	assert_eq!(metadata["python_config_vars"]["Py_DEBUG"], "0");
	assert_eq!(metadata["python_config_vars"]["Py_GIL_DISABLED"], "0");
	assert_eq!(
		pyo3_build_config::get().version().to_string(),
		metadata["python_major_minor_version"].as_str().unwrap()
	);
	assert!(
		!pyo3_build_config::get().shared(),
		"the embedded Python runtime requires the checked-in PyO3 configuration"
	);

	// Prepare the sources, frozen bootstrap, and native objects.
	let rustc = std::env::var_os("RUSTC").unwrap();
	let sysroot = Command::new(rustc)
		.args(["--print", "sysroot"])
		.output()
		.unwrap();
	assert!(sysroot.status.success());
	let sysroot = String::from_utf8(sysroot.stdout).unwrap();
	let status =
		Command::new(host_distribution.join(host_metadata["python_exe"].as_str().unwrap()))
			.args(["-E", "-s"])
			.arg("embed.py")
			.arg(&distribution)
			.arg(&host_distribution)
			.arg(&output)
			.arg(root.join("packages/clients/py/src"))
			.arg(sysroot.trim())
			.status()
			.expect("run uv sync --locked --all-packages before building the Python runtime");
	assert!(
		status.success(),
		"failed to prepare the embedded Python runtime"
	);
	let native: Value =
		serde_json::from_reader(std::fs::File::open(output.join("native.json")).unwrap()).unwrap();
	let mut build = cc::Build::new();
	build.include(distribution.join(metadata["python_paths"]["include"].as_str().unwrap()));
	build.include(&output).file(output.join("python.c"));
	for object in native["objects"].as_array().unwrap() {
		build.object(object.as_str().unwrap());
	}
	build.compile("tangram_python");

	// Link the native module dependencies after the interpreter archive.
	let mut links = BTreeSet::new();
	for link in native["links"].as_array().unwrap() {
		let name = link["name"].as_str().unwrap();
		if let Some(path) = link["path_static"].as_str() {
			let path = distribution.join(path);
			println!(
				"cargo:rustc-link-search=native={}",
				path.parent().unwrap().display()
			);
			links.insert(format!("static={name}"));
		} else if link["framework"].as_bool() == Some(true) {
			links.insert(format!("framework={name}"));
		} else {
			links.insert(name.to_owned());
		}
	}
	if links.remove("static=ssl") {
		println!("cargo:rustc-link-lib=static=ssl");
	}
	for link in links {
		println!("cargo:rustc-link-lib={link}");
	}
	if target.starts_with("aarch64-") && target.contains("-linux-") {
		// libffi calls the compiler runtime to flush generated instruction caches.
		let library = build
			.get_compiler()
			.to_command()
			.arg("-print-libgcc-file-name")
			.output()
			.unwrap();
		assert!(library.status.success());
		let library = String::from_utf8(library.stdout).unwrap();
		let library = Path::new(library.trim());
		assert!(library.is_file(), "failed to find the compiler runtime");
		println!(
			"cargo:rustc-link-search=native={}",
			library.parent().unwrap().display()
		);
		let name = library.file_stem().unwrap().to_str().unwrap();
		let name = name.strip_prefix("lib").unwrap();
		println!("cargo:rustc-link-lib=static={name}");
	}
	if target.ends_with("apple-darwin") {
		println!(
			"cargo:rustc-link-search=native={}",
			distribution.join("build/lib").display()
		);
		println!("cargo:rustc-link-lib=static=clang_rt.osx");
	}
}

fn static_crt(target: &str) -> bool {
	// Cargo probes multiple crate types, which can hide the default static CRT setting on musl.
	let flags = std::env::var("CARGO_ENCODED_RUSTFLAGS").unwrap_or_default();
	let output = Command::new(std::env::var_os("RUSTC").unwrap())
		.args(["--print", "cfg", "--target", target, "--crate-type", "bin"])
		.args(flags.split('\u{1f}').filter(|flag| !flag.is_empty()))
		.output()
		.unwrap();
	assert!(output.status.success());
	let config = String::from_utf8(output.stdout).unwrap();
	config
		.lines()
		.any(|line| line == "target_feature=\"crt-static\"")
}

fn relative_path(from: &Path, to: &Path) -> PathBuf {
	let common = from
		.components()
		.zip(to.components())
		.take_while(|(left, right)| left == right)
		.count();
	let mut path = PathBuf::new();
	for _ in from.components().skip(common) {
		path.push("..");
	}
	for component in to.components().skip(common) {
		path.push(component);
	}
	path
}

fn download(manifest: &Value, target: &str, output: &Path, variable: &str) -> PathBuf {
	if let Some(path) = std::env::var_os(variable) {
		return PathBuf::from(path);
	}
	let artifact = manifest["targets"]
		.get(target)
		.unwrap_or_else(|| panic!("unsupported Python target: {target}"));
	let lock = std::fs::File::create(output.join("distributions.lock")).unwrap();
	lock.lock().unwrap();
	let version = manifest["python"].as_str().unwrap();
	let release = manifest["release"].as_str().unwrap();
	let directory = output.join(format!("cpython-{version}-{release}-{target}"));
	let path = directory.join("python");
	if path.join("PYTHON.json").is_file() {
		return path;
	}
	let archive = output.join(format!("cpython-{target}.tar.zst"));
	let status = Command::new("curl")
		.args(["--location", "--fail", "--retry", "3", "--output"])
		.arg(&archive)
		.arg(artifact["url"].as_str().unwrap())
		.status()
		.unwrap();
	assert!(status.success(), "failed to download CPython for {target}");
	let digest = Sha256::digest(std::fs::read(&archive).unwrap());
	let digest = digest.iter().fold(String::new(), |mut digest, byte| {
		write!(digest, "{byte:02x}").unwrap();
		digest
	});
	assert_eq!(
		digest,
		artifact["sha256"].as_str().unwrap(),
		"the CPython archive checksum does not match"
	);
	let temporary = directory.with_extension("tmp");
	std::fs::remove_dir_all(&temporary).ok();
	std::fs::create_dir_all(&temporary).unwrap();
	let status = Command::new("tar")
		.args(["--extract", "--zstd", "--file"])
		.arg(&archive)
		.current_dir(&temporary)
		.status()
		.unwrap();
	assert!(status.success(), "failed to extract CPython for {target}");
	std::fs::rename(temporary, directory).unwrap();
	path
}
