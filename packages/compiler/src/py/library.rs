use {
	super::system::System,
	include_dir::{Dir, include_dir},
	tangram_client::prelude::*,
};

static LIBRARY: Dir = include_dir!("$OUT_DIR/py");

pub(super) fn load(system: &System) -> tg::Result<()> {
	copy(system, &LIBRARY)?;
	system
		.memory
		.write_file(
			"/library/__builtins__.pyi",
			include_str!("__builtins__.pyi"),
		)
		.map_err(|error| tg::error!(!error, "failed to load the python globals"))?;
	Ok(())
}

fn copy(system: &System, directory: &Dir) -> tg::Result<()> {
	let path = format!("/library/{}", directory.path().display());
	system
		.memory
		.create_directory_all(path.as_str())
		.map_err(|error| tg::error!(!error, "failed to create the python library directory"))?;
	for file in directory.files() {
		if !file
			.path()
			.extension()
			.is_some_and(|extension| extension == "py" || extension == "pyi")
			&& file
				.path()
				.file_name()
				.is_none_or(|name| name != "py.typed")
		{
			continue;
		}
		let path = format!("/library/{}", file.path().display());
		let text = file
			.contents_utf8()
			.ok_or_else(|| tg::error!("invalid python library source"))?;
		system
			.memory
			.write_file(path.as_str(), text)
			.map_err(|error| tg::error!(!error, "failed to load the python library source"))?;
	}
	for directory in directory.dirs() {
		copy(system, directory)?;
	}
	Ok(())
}
