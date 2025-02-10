fn main() {
	println!("cargo:rerun-if-env-changed=TANGRAM_CLI_COMMIT_HASH");
}
