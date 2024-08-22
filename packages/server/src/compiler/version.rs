use super::{document::Document, Compiler};
use tangram_client as tg;

impl Compiler {
	pub async fn get_module_version(&self, module: &tg::module::Reference) -> tg::Result<i32> {
		// Get the entry for the document.
		let entry = self.documents.entry(module.clone());

		// If there is an open document, return its version.
		if let dashmap::Entry::Occupied(entry) = &entry {
			let document = entry.get();
			if document.open {
				return Ok(document.version);
			}
		}

		// Get the path.
		let (
			tg::module::Kind::Js
			| tg::module::Kind::Ts
			| tg::module::Kind::Artifact
			| tg::module::Kind::Directory
			| tg::module::Kind::File
			| tg::module::Kind::Symlink,
			tg::module::Source::Path(path),
		) = (module.kind(), module.source())
		else {
			return Ok(0);
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
