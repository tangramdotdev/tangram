use {
	crate::Command,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub fn prepare_command_for_spawn(
	command: &mut crate::Command,
	tangram_path: &Path,
	_library_paths: &[PathBuf],
) -> tg::Result<()> {
	if !command.env.contains_key("HOME") {
		command.env.insert("HOME".to_owned(), "/root".to_owned());
	}
	super::append_directories_to_path(
		command,
		&[
			Path::new("/opt/tangram/bin"),
			Path::new("/usr/bin"),
			Path::new("/bin"),
		],
	)?;

	let target = crate::Sandbox::guest_tangram_path_from_host_tangram_path(tangram_path);
	if !command.env.contains_key("SSL_CERT_DIR") && command_resolves_to_path(command, &target) {
		command.env.insert(
			"SSL_CERT_DIR".to_owned(),
			"/opt/tangram/etc/ssl/certs".to_owned(),
		);
	}
	Ok(())
}

fn command_resolves_to_path(command: &Command, target: &Path) -> bool {
	let resolved = if command.executable.is_absolute() {
		command.executable.clone()
	} else {
		let Some(path) = command.env.get("PATH") else {
			return false;
		};
		let Some(resolved) = crate::util::which(Path::new(path), &command.executable) else {
			return false;
		};
		resolved
	};
	canonicalized_paths_match(&resolved, target)
}

fn canonicalized_paths_match(lhs: &Path, rhs: &Path) -> bool {
	let Ok(lhs) = std::fs::canonicalize(lhs) else {
		return false;
	};
	let Ok(rhs) = std::fs::canonicalize(rhs) else {
		return false;
	};
	lhs == rhs
}
