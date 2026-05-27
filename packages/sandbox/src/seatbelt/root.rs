use {
	crate::libraries,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub path: PathBuf,
	pub tangram_path: PathBuf,
}

pub fn create(arg: &Arg) -> tg::Result<()> {
	if let Err(error) = std::fs::remove_dir_all(&arg.path)
		&& error.kind() != std::io::ErrorKind::NotFound
	{
		return Err(tg::error!(
			!error,
			path = %arg.path.display(),
			"failed to remove the sandbox directory"
		));
	}
	std::fs::create_dir_all(&arg.path)
		.map_err(|error| tg::error!(!error, "failed to create the sandbox directory"))?;
	let libraries = libraries::resolve(&arg.tangram_path)?;
	let libraries_path = arg.path.join("lib");
	libraries::stage(&libraries_path, &libraries)?;
	prepare_tangram_wrapper(arg, &libraries_path)?;
	Ok(())
}

fn prepare_tangram_wrapper(arg: &Arg, libraries_path: &Path) -> tg::Result<()> {
	let bin_path = arg.path.join("bin");
	std::fs::create_dir_all(&bin_path)
		.map_err(|error| tg::error!(!error, "failed to create the sandbox bin directory"))?;
	let tangram_wrapper_path = bin_path.join("tangram");
	let tangram_path = shell_quote(&arg.tangram_path);
	let libraries_path = shell_quote(libraries_path);
	let wrapper = format!(
		r#"#!/bin/sh
if [ -n "${{DYLD_LIBRARY_PATH:-}}" ]; then
	export DYLD_LIBRARY_PATH='{libraries_path}':"$DYLD_LIBRARY_PATH"
else
	export DYLD_LIBRARY_PATH='{libraries_path}'
fi
if [ -n "${{DYLD_FALLBACK_LIBRARY_PATH:-}}" ]; then
	export DYLD_FALLBACK_LIBRARY_PATH='{libraries_path}':"$DYLD_FALLBACK_LIBRARY_PATH"
else
	export DYLD_FALLBACK_LIBRARY_PATH='{libraries_path}'
fi
exec '{tangram_path}' "$@"
"#,
	);
	std::fs::write(&tangram_wrapper_path, wrapper)
		.map_err(|error| tg::error!(!error, "failed to write the tangram wrapper"))?;
	let permissions = std::os::unix::fs::PermissionsExt::from_mode(0o755);
	std::fs::set_permissions(&tangram_wrapper_path, permissions)
		.map_err(|error| tg::error!(!error, "failed to set the tangram wrapper permissions"))?;
	let tg_wrapper_path = bin_path.join("tg");
	std::fs::remove_file(&tg_wrapper_path).ok();
	std::os::unix::fs::symlink("tangram", &tg_wrapper_path)
		.map_err(|error| tg::error!(!error, "failed to create the tg wrapper symlink"))?;
	Ok(())
}

fn shell_quote(path: &Path) -> String {
	path.to_string_lossy().replace('\'', r"'\''")
}
