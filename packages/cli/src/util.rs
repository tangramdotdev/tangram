use std::path::{Path, PathBuf};
use tangram_client as tg;

pub fn path_diff(src: &Path, dst: &Path) -> tg::Result<PathBuf> {
	if !src.is_absolute() || !dst.is_absolute() {
		return Err(tg::error!("both paths must be absolute"));
	}
	let src_components: Vec<_> = src.components().collect();
	let dst_components: Vec<_> = dst.components().collect();
	let common_prefix_len = src_components
		.iter()
		.zip(dst_components.iter())
		.take_while(|(a, b)| a == b)
		.count();
	let parents_needed = src_components.len() - common_prefix_len;
	let mut result = PathBuf::new();
	for _ in 0..parents_needed {
		result.push("..");
	}
	for component in &dst_components[common_prefix_len..] {
		result.push(component);
	}
	Ok(result)
}
