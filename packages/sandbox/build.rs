use std::{path::PathBuf, process::Command};

fn main() {
	println!("cargo:rerun-if-changed=build.rs");

	let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
	if target_os != "linux" {
		return;
	}

	let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
	let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());

	let archive_name = match target_arch.as_str() {
		"aarch64" => "sandbox_aarch64_linux.tar.zst",
		"x86_64" => "sandbox_x86_64_linux.tar.zst",
		_ => panic!("unsupported Linux target architecture: {target_arch}"),
	};
	let archive_path = out_dir.join(archive_name);
	let url = format!(
		"https://github.com/tangramdotdev/bootstrap/releases/download/v2026.01.26/{archive_name}"
	);
	let status = Command::new("curl")
		.args(["--location", "--fail", "--output"])
		.arg(&archive_path)
		.arg(url)
		.status()
		.unwrap();
	assert!(status.success(), "failed to download {archive_name}");
	let output_path = out_dir.join("rootfs");
	std::fs::remove_dir_all(&output_path).ok();
	std::fs::create_dir_all(&output_path).unwrap();
	let status = Command::new("tar")
		.arg("--extract")
		.arg("--zstd")
		.arg("--file")
		.arg(&archive_path)
		.current_dir(&output_path)
		.status()
		.unwrap();
	assert!(status.success(), "failed to extract the sandbox archive");
}
