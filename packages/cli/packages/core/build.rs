fn main() {
	// Get the commit hash from the environment, or from the file that a sandboxed build adds to the source.
	println!("cargo:rerun-if-changed=build.rs");
	println!("cargo:rerun-if-env-changed=TANGRAM_CLI_COMMIT_HASH");
	let mut commit = std::env::var("TANGRAM_CLI_COMMIT_HASH").ok();
	if commit.is_none()
		&& let Ok(contents) = std::fs::read_to_string("commit")
	{
		println!("cargo:rerun-if-changed=commit");
		commit = Some(contents);
	}

	// Give the commit hash to the compiler.
	let Some(commit) = commit else {
		return;
	};
	let commit = commit.trim();
	if !commit.is_empty() {
		println!("cargo:rustc-env=TANGRAM_CLI_COMMIT_HASH={commit}");
	}
}
