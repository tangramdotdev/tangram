use crate::{util::path::Ext, Server};
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(crate) async fn root_module_for_package(
		&self,
		package: Either<tg::directory::Id, PathBuf>,
	) -> tg::Result<tg::module::Data> {
		match package {
			Either::Left(package) => {
				// Get the root module file name.
				let root_module_name = tg::Directory::with_id(package.clone())
					.entries(self)
					.await
					.map_err(
						|source| tg::error!(!source, %package, "failed to get directory entries"),
					)?
					.keys()
					.find(|&name| tg::package::is_root_module_path(name.as_ref()))
					.cloned()
					.ok_or_else(|| tg::error!("could not find root module in package"))?;

				// Infer the kind.
				let kind = infer_module_kind(&root_module_name)?;

				// Create the module.
				Ok(tg::module::Data {
					kind,
					referent: tg::Referent {
						item: tg::module::data::Item::Object(package.clone().into()),
						subpath: Some(root_module_name.into()),
						tag: None,
					},
				})
			},
			Either::Right(path) => {
				// Get the root module file name.
				let root_module_file_name =
					try_get_root_module_name_for_path(&path).await?.ok_or_else(
						|| tg::error!(%package = path.display(), "missing root module file"),
					)?;

				// Infer the kind.
				let kind = infer_module_kind(&root_module_file_name)?;

				// Create the module
				Ok(tg::module::Data {
					kind,
					referent: tg::Referent {
						item: tg::module::data::Item::Path(path),
						subpath: Some(root_module_file_name.into()),
						tag: None,
					},
				})
			},
		}
	}

	pub(crate) async fn module_for_path(&self, path: &Path) -> tg::Result<tg::module::Data> {
		// Find the lockfile.
		let (lockfile_path, lockfile) = 'a: {
			for ancestor in path.ancestors().skip(1) {
				// Check if the lockfile exists.
				let lockfile_path = ancestor.join(tg::package::LOCKFILE_FILE_NAME);
				let exists = tokio::fs::try_exists(&lockfile_path)
					.await
					.map_err(|source| tg::error!(!source, "failed to check if lockfile exists"))?;
				if !exists {
					continue;
				}

				// Parse the lockfile.
				let contents = tokio::fs::read_to_string(&lockfile_path).await.map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to read lockfile"),
				)?;
				let lockfile = serde_json::from_str::<tg::Lockfile>(&contents).map_err(
					|source| tg::error!(!source, %path = lockfile_path.display(), "failed to deserialize lockfile"),
				)?;
				break 'a (lockfile_path, lockfile);
			}
			return Err(tg::error!("could not find a lockfile"));
		};

		// Find the package within the lockfile.
		let package = self
			.find_node_in_lockfile(Either::Right(path), &lockfile_path, &lockfile)
			.await?
			.package;

		// Get the file name.
		let file_name = path
			.file_name()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?
			.to_str()
			.ok_or_else(|| tg::error!(%path = path.display(), "invalid path"))?;
		if !tg::package::is_module_path(file_name.as_ref()) {
			return Err(tg::error!(%path = path.display(), "expected a module path"));
		}

		// Get the kind.
		let kind = infer_module_kind(file_name)?;

		// Get the subpath.
		let subpath = path.diff(&package).unwrap();

		// Create the module.
		Ok(tg::module::Data {
			kind,
			referent: tg::Referent {
				item: tg::module::data::Item::Path(package.clone()),
				subpath: Some(subpath),
				tag: None,
			},
		})
	}
}

#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn infer_module_kind(name: &str) -> tg::Result<tg::module::Kind> {
	if name.ends_with(".d.ts") {
		Ok(tg::module::Kind::Dts)
	} else if name.ends_with(".ts") {
		Ok(tg::module::Kind::Ts)
	} else if name.ends_with(".js") {
		Ok(tg::module::Kind::Js)
	} else {
		Err(tg::error!(%file = name, "unknown or missing file extension"))
	}
}

async fn try_get_root_module_name_for_path(path: &Path) -> tg::Result<Option<String>> {
	// Collect the file names of the directory.
	let mut entries = tokio::fs::read_dir(&path).await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to read directory"),
	)?;
	let mut file_names = Vec::new();
	while let Some(entry) = entries.next_entry().await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to get directory entry"),
	)? {
		let Some(file_name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
			continue;
		};
		file_names.push(file_name);
	}

	// Sort to ensure a stable order/precedence.
	file_names.sort();

	// Get the root module file name.
	let root_module_file_name = file_names
		.into_iter()
		.find(|name| tg::package::is_root_module_path(name.as_ref()));
	Ok(root_module_file_name)
}
