use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub fn prepare_command_for_spawn(
	command: &mut crate::Command,
	_tangram_path: &Path,
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
	)
}
