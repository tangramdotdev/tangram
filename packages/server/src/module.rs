use crate::Server;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(crate) async fn root_module_for_package(
		&self,
		package: Either<tg::directory::Id, PathBuf>,
	) -> tg::Result<tg::Module> {
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
				Ok(tg::Module {
					kind,
					referent: tg::Referent {
						item: tg::module::Item::Object(package.clone().into()),
						path: None,
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
				Ok(tg::Module {
					kind,
					referent: tg::Referent {
						item: tg::module::Item::Path(path),
						path: None,
						subpath: Some(root_module_file_name.into()),
						tag: None,
					},
				})
			},
		}
	}

	pub(crate) async fn module_for_path(&self, path: &Path) -> tg::Result<tg::Module> {
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
		let subpath = path.strip_prefix(&package).unwrap().to_owned();

		// Create the module.
		Ok(tg::Module {
			kind,
			referent: tg::Referent {
				item: tg::module::Item::Path(package.clone()),
				path: None,
				subpath: Some(subpath),
				tag: None,
			},
		})
	}
}

pub(crate) fn infer_module_kind(path: impl AsRef<Path>) -> tg::Result<tg::module::Kind> {
	let path = path.as_ref();
	if path.ends_with(".d.ts") {
		Ok(tg::module::Kind::Dts)
	} else if path.extension().is_some_and(|ext| ext == "ts") {
		Ok(tg::module::Kind::Ts)
	} else if path.extension().is_some_and(|ext| ext == "js") {
		Ok(tg::module::Kind::Js)
	} else {
		Err(tg::error!(%path = path.display(), "unknown or missing file extension"))
	}
}

async fn try_get_root_module_name_for_path(path: &Path) -> tg::Result<Option<String>> {
	// Collect the file names of the directory.
	let mut file_names = Vec::new();
	let mut read_dir = tokio::fs::read_dir(&path).await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to read directory"),
	)?;
	while let Some(entry) = read_dir.next_entry().await.map_err(
		|source| tg::error!(!source, %path = path.display(), "failed to get directory entry"),
	)? {
		let Some(file_name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
			continue;
		};
		file_names.push(file_name);
	}
	file_names.sort();
	let root_module_file_name = file_names
		.into_iter()
		.find(|name| tg::package::is_root_module_path(name.as_ref()));
	Ok(root_module_file_name)
}
