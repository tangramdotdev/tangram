fn main() {
	println!("cargo::rerun-if-changed=build.rs");
	println!("cargo::rerun-if-changed=download.nu");
	let status = std::process::Command::new("nu")
		.arg("download.nu")
		.status()
		.unwrap();
	if !status.success() {
		panic!();
	}
}
