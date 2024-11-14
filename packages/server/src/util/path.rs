use std::path::{Path, PathBuf};
use tangram_client as tg;

pub fn diff(src: &Path, dst: &Path) -> tg::Result<PathBuf> {
	// Ensure both paths are absolute.
	if !src.is_absolute() || !dst.is_absolute() {
		return Err(tg::error!("both paths must be absolute"));
	}

	// Convert paths to components.
	let src_components: Vec<_> = src.components().collect();
	let dst_components: Vec<_> = dst.components().collect();

	// Find the common prefix length.
	let common_prefix_len = src_components
		.iter()
		.zip(dst_components.iter())
		.take_while(|(a, b)| a == b)
		.count();

	// Calculate number of parent directory traversals needed
	let parents_needed = src_components.len() - common_prefix_len;

	// Build the relative path
	let mut result = PathBuf::new();

	// Add parent directory traversals
	for _ in 0..parents_needed {
		result.push("..");
	}

	// Add remaining components from target path
	for component in &dst_components[common_prefix_len..] {
		result.push(component);
	}

	Ok(result)
}
