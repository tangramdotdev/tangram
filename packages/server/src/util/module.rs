use tangram_client as tg;
pub async fn try_get_root_module_path_for_path(
	path: &std::path::Path,
) -> tg::Result<Option<tg::Path>> {
	let mut output = None;
	for name in tg::module::ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(name))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
		{
			if output.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			output = Some(name.parse().unwrap());
		}
	}
	Ok(output)
}

pub async fn get_root_module_path_for_path(path: &std::path::Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

#[must_use]
pub fn is_root_module_path(path: &std::path::Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let name = name.to_string_lossy();
	tg::module::ROOT_MODULE_FILE_NAMES
		.iter()
		.any(|n| name == *n)
}

#[must_use]
pub fn is_module_path(path: &std::path::Path) -> bool {
	let Some(name) = path.file_name() else {
		return false;
	};
	let name = name.to_string_lossy();
	tg::module::ROOT_MODULE_FILE_NAMES
		.iter()
		.any(|n| name == *n)
		|| name.ends_with(".tg.js")
		|| name.ends_with(".tg.ts")
}
