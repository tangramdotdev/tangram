use crate as tg;
use std::path::Path;

pub async fn get_root_module_path<H>(handle: &H, package: &tg::Directory) -> tg::Result<tg::Path>
where
	H: tg::Handle,
{
	try_get_root_module_path(handle, package)
		.await?
		.ok_or_else(|| tg::error!("failed to find the package's root module"))
}

pub async fn try_get_root_module_path<H>(
	handle: &H,
	package: &tg::Directory,
) -> tg::Result<Option<tg::Path>>
where
	H: tg::Handle,
{
	let mut root_module_path = None;
	for module_file_name in tg::package::ROOT_MODULE_FILE_NAMES {
		if package
			.try_get(handle, &module_file_name.parse().unwrap())
			.await?
			.is_some()
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}

pub async fn get_root_module_path_for_path(path: &Path) -> tg::Result<tg::Path> {
	try_get_root_module_path_for_path(path).await?.ok_or_else(
		|| tg::error!(%path = path.display(), "failed to find the package's root module"),
	)
}

pub async fn try_get_root_module_path_for_path(path: &Path) -> tg::Result<Option<tg::Path>> {
	let mut root_module_path = None;
	for module_file_name in tg::package::ROOT_MODULE_FILE_NAMES {
		if tokio::fs::try_exists(path.join(module_file_name))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?
		{
			if root_module_path.is_some() {
				return Err(tg::error!("found multiple root modules"));
			}
			root_module_path = Some(module_file_name.parse().unwrap());
		}
	}
	Ok(root_module_path)
}
