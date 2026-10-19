use {
	crate::{
		Compiler,
		document::{Document, Key, Lockfile},
	},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

impl Compiler {
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Data,
		specifier: &str,
		attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<tg::module::Data> {
		let import = tg::module::Import::with_specifier_and_attributes(specifier, attributes)
			.map_err(|error| tg::error!(!error, "failed to create the import"))?;
		let arg = tg::module::resolve::Arg {
			import: import.clone(),
			referrer: Some(referrer.clone()),
		};
		let output = self.instance.resolve_module(arg).await.map_err(|error| {
			tg::error!(
				source = error,
				import = ?import.without_token(),
				referrer = ?referrer.without_token(),
				"failed to resolve specifier relative to the module"
			)
		})?;

		Ok(output.module)
	}

	pub async fn has_invalidated_module_resolutions(&self, module: &tg::module::Data) -> bool {
		let tg::module::data::Source::Path(_) = &module.referent.node else {
			return false;
		};

		// Get the resolution version last consumed by the language service.
		let document = self
			.documents
			.entry(Key::new(module))
			.or_insert_with(|| Document {
				dirty: false,
				lockfile: None,
				modified: None,
				module: module.clone(),
				open: false,
				revision: 0,
				text: None,
				version: 0,
			});
		let previous = document.lockfile.clone();
		drop(document);

		// Compare the path and full timestamp, including lockfile creation and removal.
		let current = self.get_module_resolution_version(module).await;

		current != previous
	}

	pub async fn validate_module_resolutions(&self, module: &tg::module::Data) {
		let tg::module::data::Source::Path(_) = &module.referent.node else {
			return;
		};
		let lockfile = self.get_module_resolution_version(module).await;
		if let Some(mut document) = self.documents.get_mut(&Key::new(module)) {
			document.lockfile = lockfile;
		}
	}

	pub async fn get_module_resolution_version(
		&self,
		module: &tg::module::Data,
	) -> Option<Lockfile> {
		let tg::module::data::Source::Path(path) = &module.referent.node else {
			return None;
		};
		self.find_lockfile_for_path(path).await
	}
}
