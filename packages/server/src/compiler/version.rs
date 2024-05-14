use super::{document::Document, Compiler};
use dashmap::mapref::entry::Entry;
use tangram_client as tg;

impl Compiler {
	pub async fn get_module_version(&self, module: &tg::Module) -> tg::Result<i32> {
		// Get the entry for the document.
		let entry = self.documents.entry(module.clone());

		// If there is an open document, return its version.
		if let Entry::Occupied(entry) = &entry {
			let document = entry.get();
			if document.open {
				return Ok(document.version);
			}
		}

		// Get the path.
		let path = match module {
			tg::Module::Js(tg::module::Js::PackagePath(package_path))
			| tg::Module::Ts(tg::module::Js::PackagePath(package_path)) => {
				package_path.package_path.join(&package_path.path)
			},

			tg::Module::Artifact(tg::module::Artifact::Path(path))
			| tg::Module::Directory(tg::module::Directory::Path(path))
			| tg::Module::File(tg::module::File::Path(path))
			| tg::Module::Symlink(tg::module::Symlink::Path(path)) => path.clone().into(),

			// If the module has no path, then it is immutable.
			_ => return Ok(0),
		};

		// Get the modified time.
		let metadata = tokio::fs::metadata(&path)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the metadata"))?;
		let modified = metadata.modified().map_err(|error| {
			tg::error!(source = error, "failed to get the last modification time")
		})?;

		// Get or create the document.
		let mut document = entry.or_insert(Document {
			open: false,
			version: 0,
			modified: Some(modified),
			text: None,
		});

		// Update the modified time if necessary.
		if modified > document.modified.unwrap() {
			document.modified = Some(modified);
			document.version += 1;
		}

		Ok(document.version)
	}
}
