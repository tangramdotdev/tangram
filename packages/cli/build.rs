#[cfg(feature = "foundationdb")]
use std::{path::Path, process::Command};

fn main() {
	println!("cargo:rerun-if-changed=build.rs");
	#[cfg(feature = "foundationdb")]
	fdb();
}

#[cfg(feature = "foundationdb")]
fn fdb() {
	let target_arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap();
	let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap();
	let out_dir = std::env::var("OUT_DIR").unwrap();
	let fdb_dir = Path::new(&out_dir).join("fdb");
	std::fs::create_dir_all(&fdb_dir).unwrap();
	let fdb_version = "7.3.58";
	match target_os.as_str() {
		"linux" => {
			let lib_url = format!(
				"https://github.com/apple/foundationdb/releases/download/{fdb_version}/libfdb_c.{target_arch}.so",
			);
			let status = Command::new("curl")
				.args(["-L", "-o", "libfdb_c.so", &lib_url])
				.current_dir(&fdb_dir)
				.status()
				.unwrap();
			assert!(status.success());
			println!("cargo:rustc-link-search=native={}", fdb_dir.display());
		},
		"macos" => {
			let arch = match target_arch.as_str() {
				"aarch64" => "arm64",
				"x86_64" => "x86_64",
				_ => unreachable!(),
			};
			let url = format!(
				"https://github.com/apple/foundationdb/releases/download/{fdb_version}/FoundationDB-{fdb_version}_{arch}.pkg"
			);
			let status = Command::new("curl")
				.args(["-L", "-o", "fdb.pkg", &url])
				.current_dir(&fdb_dir)
				.status()
				.unwrap();
			assert!(status.success());
			let status = Command::new("xar")
				.args(["-xf", "fdb.pkg"])
				.current_dir(&fdb_dir)
				.status()
				.unwrap();
			assert!(status.success());
			let mut archive = Command::new("gunzip")
				.args(["-c", "FoundationDB-clients.pkg/Payload"])
				.current_dir(&fdb_dir)
				.stdout(std::process::Stdio::piped())
				.spawn()
				.unwrap();
			let status = Command::new("cpio")
				.args(["-idv"])
				.current_dir(&fdb_dir)
				.stdin(archive.stdout.take().unwrap())
				.status()
				.unwrap();
			assert!(status.success());
			let status = archive.wait().unwrap();
			assert!(status.success());
			println!(
				"cargo:rustc-link-search=native={}",
				fdb_dir.join("usr/local/lib").display()
			);
		},
		_ => unreachable!(),
	}
}
