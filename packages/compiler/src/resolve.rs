use {
	crate::{
		Compiler,
		document::{Document, Key, Lockfile},
	},
	std::collections::{BTreeMap, HashMap},
	tangram_client::prelude::*,
};

/// The resolutions of a module's imports to objects at a version of the module and its lockfile.
pub(crate) struct Resolutions {
	lockfile: Option<Lockfile>,
	modules: HashMap<tg::module::Import, tg::module::Data>,
	version: u64,
}

impl Compiler {
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Data,
		specifier: &str,
		attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<tg::module::Data> {
		let import = tg::module::Import::with_specifier_and_attributes(specifier, attributes)
			.map_err(|error| tg::error!(!error, "failed to create the import"))?;
		self.resolve_import(referrer, import).await
	}

	/// Resolve an import of a module, reusing a resolution to an object while the module and its lockfile are unchanged.
	pub(crate) async fn resolve_import(
		&self,
		referrer: &tg::module::Data,
		import: tg::module::Import,
	) -> tg::Result<tg::module::Data> {
		// Get the resolution if the module and its lockfile are unchanged.
		let key = Key::new(referrer);
		let version = self.get_module_version(referrer).await.ok();
		let lockfile = self.get_module_resolution_version(referrer).await;
		if let Some(version) = version
			&& let Some(resolutions) = self.resolutions.get(&key)
			&& resolutions.version == version
			&& resolutions.lockfile == lockfile
			&& let Some(module) = resolutions.modules.get(&import)
		{
			return Ok(module.clone());
		}

		// Resolve the import.
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

		// Store a resolution to an object, replacing the resolutions of an older version of the module. A resolution to a path depends on the file system, so it is not stored.
		if let Some(version) = version
			&& let tg::module::data::Source::Edge(_) = &output.module.referent.node
		{
			let mut resolutions = self.resolutions.entry(key).or_insert_with(|| Resolutions {
				lockfile: lockfile.clone(),
				modules: HashMap::new(),
				version,
			});
			if resolutions.version != version || resolutions.lockfile != lockfile {
				resolutions.lockfile = lockfile;
				resolutions.modules.clear();
				resolutions.version = version;
			}
			resolutions.modules.insert(import, output.module.clone());
		}

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
				revision: self.next_revision(),
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
