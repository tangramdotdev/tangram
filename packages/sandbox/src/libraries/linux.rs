use {
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

pub(super) fn resolve(tangram_path: &Path) -> tg::Result<Vec<PathBuf>> {
	let mut libraries = Vec::new();
	resolve_linux(&mut libraries, tangram_path)?;
	Ok(libraries)
}

fn resolve_linux(libraries: &mut Vec<PathBuf>, path: &Path) -> tg::Result<()> {
	let output = std::process::Command::new("ldd")
		.arg(path)
		.output()
		.map_err(|error| {
			tg::error!(
				!error,
				path = %path.display(),
				"failed to inspect the dynamic library dependencies"
			)
		})?;
	if !output.status.success() {
		return Err(tg::error!(
			path = %path.display(),
			"failed to inspect the dynamic library dependencies"
		));
	}
	let mut names = libraries
		.iter()
		.filter_map(|path| path.file_name())
		.map(std::borrow::ToOwned::to_owned)
		.collect::<std::collections::BTreeSet<_>>();
	for line in String::from_utf8_lossy(&output.stdout).lines() {
		let Some(dependency) = parse_ldd_line(line)? else {
			continue;
		};
		let Some(name) = dependency.file_name() else {
			continue;
		};
		if names.insert(name.to_owned()) {
			libraries.push(dependency);
		}
	}
	Ok(())
}

fn parse_ldd_line(line: &str) -> tg::Result<Option<PathBuf>> {
	let line = line.trim();
	if line.is_empty() || line.starts_with("linux-vdso.so") {
		return Ok(None);
	}
	if let Some((name, target)) = line.split_once("=>") {
		let target = target.trim();
		if target == "not found" {
			return Err(tg::error!(
				library = %name.trim(),
				"failed to find a dynamic library dependency"
			));
		}
		return Ok(target
			.split_whitespace()
			.next()
			.map(PathBuf::from)
			.filter(|path| path.is_absolute()));
	}
	Ok(line
		.split_whitespace()
		.next()
		.map(PathBuf::from)
		.filter(|path| path.is_absolute()))
}
