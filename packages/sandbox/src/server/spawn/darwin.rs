use {crate::Command, std::path::Path, tangram_client::prelude::*};

pub fn prepare_command_for_spawn(
	command: &mut crate::Command,
	tangram_path: &Path,
	library_paths: &[std::path::PathBuf],
) -> tg::Result<()> {
	if !command.env.contains_key("HOME") {
		// Use `/root` to match the Linux container default. The seatbelt
		// sandbox does not chroot, so the host's `$HOME` (typically under
		// `/Users`) is visible but denied by the profile; tools that stat
		// `$HOME/.gitconfig` (for example libgit2 via `cargo`) then see
		// `EPERM` instead of `ENOENT` and treat the config as invalid. A
		// path that does not exist on the host under a granted subtree
		// produces `ENOENT`, which is the expected "no config" signal.
		command.env.insert("HOME".to_owned(), "/root".to_owned());
	}
	let mut paths = Vec::new();
	if let Some(wrapper_directory) = tangram_wrapper_directory(library_paths) {
		paths.push(wrapper_directory);
	}
	if let Some(parent) = tangram_path.parent() {
		paths.push(parent.to_owned());
	}
	paths.push(Path::new("/usr/bin").to_owned());
	paths.push(Path::new("/bin").to_owned());
	let paths = paths
		.iter()
		.map(std::path::PathBuf::as_path)
		.collect::<Vec<_>>();
	super::append_directories_to_path(command, &paths)?;

	if library_paths.is_empty() || !command_resolves_to_path(command, tangram_path) {
		return Ok(());
	}
	let mut paths = library_paths.to_vec();
	if let Some(existing) = command.env.get("DYLD_LIBRARY_PATH") {
		paths.extend(std::env::split_paths(existing));
	}
	let path = std::env::join_paths(paths)
		.map_err(|source| tg::error!(!source, "failed to build `DYLD_LIBRARY_PATH`"))?;
	let path = path
		.to_str()
		.ok_or_else(|| tg::error!("failed to encode `DYLD_LIBRARY_PATH` as valid UTF-8"))?;
	command
		.env
		.insert("DYLD_LIBRARY_PATH".to_owned(), path.to_owned());
	Ok(())
}

fn tangram_wrapper_directory(library_paths: &[std::path::PathBuf]) -> Option<std::path::PathBuf> {
	let rootfs_path = library_paths.first()?.parent()?;
	Some(rootfs_path.join("bin"))
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
