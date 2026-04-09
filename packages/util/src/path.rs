use std::path::{Path, PathBuf};

pub fn diff(src: &Path, dst: &Path) -> std::io::Result<Option<PathBuf>> {
	if !src.is_absolute() || !dst.is_absolute() {
		return Err(std::io::Error::other("both paths must be absolute"));
	}
	let src_components: Vec<_> = src.components().collect();
	let dst_components: Vec<_> = dst.components().collect();
	let common_prefix_components_len = src_components
		.iter()
		.zip(dst_components.iter())
		.take_while(|(a, b)| a == b)
		.count();
	let parents_needed = src_components.len() - common_prefix_components_len;
	let mut output = PathBuf::new();
	for _ in 0..parents_needed {
		output.push("..");
	}
	for component in &dst_components[common_prefix_components_len..] {
		output.push(component);
	}
	if output.as_os_str().is_empty() {
		return Ok(None);
	}
	Ok(Some(output))
}

pub fn normalize(src: impl AsRef<Path>) -> PathBuf {
	let mut components = Vec::new();
	for component in src.as_ref().components() {
		match (component, components.last()) {
			(std::path::Component::CurDir, _) => (),
			(std::path::Component::ParentDir, Some(std::path::Component::Normal(_))) => {
				components.pop();
			},
			(component, _) => components.push(component),
		}
	}
	components.into_iter().collect()
}

#[cfg(test)]
mod test {
	use std::path::PathBuf;

	#[test]
	fn normalize() {
		let path = "../foo/bar";
		assert_eq!(PathBuf::from(path), super::normalize(path));

		let path = "../foo/../bar";
		assert_eq!(PathBuf::from("../bar"), super::normalize(path));

		let path = "foo/../bar";
		assert_eq!(PathBuf::from("bar"), super::normalize(path));

		let path = "../foo/../bar/../baz";
		assert_eq!(PathBuf::from("../baz"), super::normalize(path));
	}
}
