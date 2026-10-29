use std::path::Path;

/// Visit every file under a directory without following symlinks.
pub fn walk(path: &Path, visit: &mut dyn FnMut(&Path)) {
	let Ok(read_dir) = std::fs::read_dir(path) else {
		return;
	};
	for entry in read_dir.flatten() {
		let Ok(file_type) = entry.file_type() else {
			continue;
		};
		let path = entry.path();
		if file_type.is_dir() {
			walk(&path, visit);
		} else {
			visit(&path);
		}
	}
}
