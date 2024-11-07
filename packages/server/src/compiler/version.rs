use super::{document::Document, Compiler};
use tangram_client as tg;

impl Compiler {
	pub async fn get_module_version(&self, module: &tg::Module) -> tg::Result<i32> {
		// Get the entry for the document.
		let entry = self.documents.entry(module.clone());

		// If there is an open document, then return its version.
		if let dashmap::Entry::Occupied(entry) = &entry {
			let document = entry.get();
			if document.open {
				return Ok(document.version);
			}
		}

		// Get the path.
		let tg::Module {
			kind:
				tg::module::Kind::Js
				| tg::module::Kind::Ts
				| tg::module::Kind::Artifact
				| tg::module::Kind::Directory
				| tg::module::Kind::File
				| tg::module::Kind::Symlink,
			referent:
				tg::Referent {
					item: tg::module::Item::Path(path),
					subpath,
					..
				},
			..
		} = &module
		else {
			return Ok(0);
		};
		let path = if let Some(subpath) = subpath {
			path.join(subpath)
		} else {
			path.clone()
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
			dirty: false,
			modified: Some(modified),
			open: false,
			text: None,
			version: 0,
		});

		// Update the modified time if necessary.
		if modified > document.modified.unwrap() {
			document.modified = Some(modified);
			document.version += 1;
		}

		Ok(document.version)
	}
}
