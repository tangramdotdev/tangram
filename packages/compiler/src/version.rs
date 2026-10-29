use {
	super::{
		Compiler,
		document::{Document, Key},
	},
	tangram_client::prelude::*,
};

impl Compiler {
	/// Get the version of a module as a checker in a language loads it, which for a module in another language includes its cached interface.
	pub async fn get_module_version_with_language(
		&self,
		module: &tg::module::Data,
		language: tg::module::load::Language,
	) -> tg::Result<u64> {
		self.get_module_version_with_language_inner(module, language, true)
			.await
	}

	/// Get the version of a module as a checker in a language loads it, including its cached interface only if `typed` is true.
	pub(crate) async fn get_module_version_with_language_inner(
		&self,
		module: &tg::module::Data,
		language: tg::module::load::Language,
		typed: bool,
	) -> tg::Result<u64> {
		let version = self.get_module_version(module).await?;
		#[cfg(all(feature = "javascript", feature = "python"))]
		if crate::load::generated_language(module, Some(language))
			.ok()
			.flatten()
			.is_some()
		{
			use std::hash::{Hash as _, Hasher as _};
			let interface = typed
				.then(|| self.try_get_interface(module))
				.flatten()
				.map(|entry| entry.version);
			let mut hasher = fnv::FnvHasher::default();
			(version, interface).hash(&mut hasher);
			return Ok(hasher.finish());
		}
		#[cfg(not(all(feature = "javascript", feature = "python")))]
		let _ = (language, typed);
		Ok(version)
	}

	pub async fn get_module_version(&self, module: &tg::module::Data) -> tg::Result<u64> {
		// If there is an open document, then return its version.
		let key = Key::new(module);
		let revision = self
			.documents
			.get(&key)
			.map(|document| (document.open, document.revision));
		if let Some((true, revision)) = revision {
			return Ok(revision);
		}

		// Get the path.
		let tg::module::Data {
			kind:
				tg::module::Kind::JavaScript
				| tg::module::Kind::Python
				| tg::module::Kind::TypeScript
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
			return Ok(revision.map_or(0, |(_, revision)| revision));
		};

		// Get the modified time.
		let metadata = tokio::fs::symlink_metadata(&path)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the metadata"))?;
		let modified = metadata.modified().map_err(|error| {
			tg::error!(source = error, "failed to get the last modification time")
		})?;

		// Find the lockfile.
		let lockfile = self.get_module_resolution_version(module).await;

		// Get or create the document, without holding its lock across an await.
		let mut document = self.documents.entry(key).or_insert(Document {
			dirty: false,
			lockfile,
			modified: Some(modified),
			module: module.clone(),
			open: false,
			revision: 0,
			text: None,
			version: 0,
		});
		if document.open {
			return Ok(document.revision);
		}

		// Update the modified time if necessary.
		if document.modified != Some(modified) {
			document.modified = Some(modified);
			document.revision += 1;
		}

		Ok(document.revision)
	}
}
