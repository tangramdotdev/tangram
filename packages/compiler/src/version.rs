use {
	super::{
		Compiler,
		document::{Document, Key},
	},
	tangram_client::prelude::*,
};

impl Compiler {
	pub async fn get_module_version(&self, module: &tg::module::Data) -> tg::Result<u64> {
		// Get the entry for the document.
		let entry = self.documents.entry(Key::new(module));

		// If there is an open document, then return its version.
		if let dashmap::Entry::Occupied(entry) = &entry {
			let document = entry.get();
			if document.open {
				return Ok(document.revision);
			}
		}

		// Get the path.
		let tg::module::Data {
			kind:
				tg::module::Kind::Js
				| tg::module::Kind::Py
				| tg::module::Kind::Ts
				| tg::module::Kind::Artifact
				| tg::module::Kind::Directory
				| tg::module::Kind::File
				| tg::module::Kind::Symlink,
			referent: tg::Referent {
				node: tg::module::data::Source::Path(path),
				..
			},
			..
		} = &module
		else {
			return Ok(match entry {
				dashmap::Entry::Occupied(entry) => entry.get().revision,
				dashmap::Entry::Vacant(_) => 0,
			});
		};

		// Get the modified time.
		let metadata = tokio::fs::symlink_metadata(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
		let modified = metadata.modified().map_err(|error| {
			tg::error!(source = error, "failed to get the last modification time")
		})?;

		// Find the lockfile.
		let lockfile = self.find_lockfile_for_path(path).await;

		// Get or create the document.
		let mut document = entry.or_insert(Document {
			dirty: false,
			lockfile,
			modified: Some(modified),
			module: module.clone(),
			open: false,
			revision: 0,
			text: None,
			version: 0,
		});

		// Update the modified time if necessary.
		if document.modified != Some(modified) {
			document.modified = Some(modified);
			document.revision += 1;
		}

		Ok(document.revision)
	}
}
