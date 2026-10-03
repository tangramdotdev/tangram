use std::{path::PathBuf, process::Command};

fn main() {
	let root = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("../..");
	let python = root.join(".venv/bin/python");
	let output = PathBuf::from(std::env::var("OUT_DIR").unwrap()).join("modules.json");
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-changed=embed.py");
	println!("cargo:rerun-if-changed=../clients/py/src");
	println!("cargo:rerun-if-changed=../../uv.lock");
	let config = pyo3_build_config::get();
	let status = Command::new(python)
		.arg("embed.py")
		.arg(root.join("packages/clients/py/src"))
		.arg(output)
		.arg(config.version().to_string())
		.status()
		.expect("run uv sync --locked --all-packages before building the Python runtime");
	assert!(
		status.success(),
		"failed to embed the Python client dependencies"
	);
}
