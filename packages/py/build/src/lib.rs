use {
	serde_json::Value,
	sha2::{Digest as _, Sha256},
	std::{
		fmt::Write as _,
		path::{Path, PathBuf},
		process::Command,
	},
};

#[must_use]
pub fn manifest() -> Value {
	serde_json::from_str(include_str!("../../distributions.json")).unwrap()
}

#[must_use]
pub fn host(output: &Path, host: &str) -> PathBuf {
	std::fs::create_dir_all(output).unwrap();
	let manifest = manifest();
	let distribution = download(&manifest, host, output, "TANGRAM_PYTHON_HOST_DISTRIBUTION");
	let metadata: Value =
		serde_json::from_reader(std::fs::File::open(distribution.join("PYTHON.json")).unwrap())
			.unwrap();
	assert_eq!(metadata["python_version"], manifest["python"]);
	assert_eq!(metadata["target_triple"].as_str().unwrap(), host);
	distribution.join(metadata["python_exe"].as_str().unwrap())
}

#[must_use]
pub fn packages(python: &Path, workspace: &Path, output: &Path) -> PathBuf {
	let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("packages.py");
	println!("cargo:rerun-if-changed={}", script.display());
	println!("cargo:rerun-if-env-changed=TANGRAM_PYTHON_PACKAGES");
	if let Some(path) = std::env::var_os("TANGRAM_PYTHON_PACKAGES") {
		return PathBuf::from(path)
			.canonicalize()
			.expect("invalid python packages directory");
	}
	let result = Command::new(python)
		.arg("-I")
		.arg(script)
		.arg(workspace)
		.arg(output)
		.stderr(std::process::Stdio::inherit())
		.output()
		.expect("failed to prepare the locked python packages");
	assert!(
		result.status.success(),
		"failed to prepare the locked python packages"
	);
	PathBuf::from(String::from_utf8(result.stdout).unwrap().trim())
}

#[must_use]
pub fn download(manifest: &Value, target: &str, output: &Path, variable: &str) -> PathBuf {
	if let Some(path) = std::env::var_os(variable) {
		let path = PathBuf::from(path);
		return if path.join("PYTHON.json").is_file() {
			path
		} else {
			path.join(target)
		};
	}
	let artifact = manifest["targets"]
		.get(target)
		.unwrap_or_else(|| panic!("unsupported python target: {target}"));
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
