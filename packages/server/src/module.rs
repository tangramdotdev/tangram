use crate::Server;
use std::path::Path;
use tangram_client as tg;
use tangram_either::Either;

impl Server {
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
