use {crate::Compiler, std::collections::BTreeMap, tangram_client::prelude::*, tangram_v8::Serde};

pub fn load(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<String> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let text = compiler
			.load_module_with_language(&module, Some(tg::module::load::Language::JavaScript))
			.await
			.map_err(
				|error| tg::error!(!error, module = ?module.without_token(), "failed to load the module"),
			)?;
		Ok(text)
	})
}

pub fn invalidated_resolutions(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<bool> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		// Only path modules have lockfiles.
		let tg::module::data::Source::Path(module_path) = &module.referent.node else {
			return Ok(false);
		};

		// Get or create the document.
		let document = compiler
			.documents
			.entry(crate::document::Key::new(&module))
			.or_insert_with(|| crate::document::Document {
				dirty: false,
				lockfile: None,
				modified: None,
				module: module.clone(),
				open: false,
				revision: 0,
				text: None,
				version: 0,
			});

		// Compare the path and full timestamp, including lockfile creation and removal.
		let previous = document.lockfile.clone();
		drop(document);
		let current = compiler.find_lockfile_for_path(module_path).await;
		Ok(current != previous)
	})
}

pub fn resolve(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (
		Serde<tg::module::Data>,
		String,
		Option<BTreeMap<String, String>>,
	),
) -> tg::Result<Serde<tg::module::Data>> {
	let (Serde(referrer), specifier, attributes) = args;
	let import = tg::module::Import::with_specifier_and_attributes(&specifier, attributes)
		.map_err(|error| tg::error!(!error, "failed to create the import"))?;
	compiler.main_runtime_handle.clone().block_on(async move {
		let arg = tg::module::resolve::Arg {
			referrer: Some(referrer.clone()),
			import: import.clone(),
		};
		let output = compiler
			.instance
			.resolve_module(arg)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					import = ?import.without_token(),
					referrer = ?referrer.without_token(),
					"failed to resolve specifier relative to the module"
				)
			})?;
		Ok(Serde(output.module))
	})
}

pub fn validate_resolutions(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<()> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		// Only path modules have lockfiles.
		let tg::module::data::Source::Path(path) = &module.referent.node else {
			return Ok(());
		};

		// Find the current lockfile.
		let lockfile = compiler.find_lockfile_for_path(path).await;

		// Update the document's lockfile state.
		if let Some(mut document) = compiler
			.documents
			.get_mut(&crate::document::Key::new(&module))
		{
			document.lockfile = lockfile;
		}

		Ok(())
	})
}

pub fn version(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<String> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let version = compiler
			.get_module_version(&module)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the module version"))?;
		Ok(version.to_string())
	})
}
