use {crate::Compiler, std::collections::BTreeMap, tangram_client as tg, tangram_v8::Serde};

pub fn load(
	compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<String> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let text = compiler
			.load_module(&module)
			.await
			.map_err(|source| tg::error!(!source, ?module, "failed to load the module"))?;
		Ok(text)
	})
}

pub fn resolve(
	compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	args: (
		Serde<tg::module::Data>,
		String,
		Option<BTreeMap<String, String>>,
	),
) -> tg::Result<Serde<tg::module::Data>> {
	let (Serde(referrer), specifier, attributes) = args;
	let import = tg::module::Import::with_specifier_and_attributes(&specifier, attributes)
		.map_err(|source| tg::error!(!source, "failed to create the import"))?;
	compiler.main_runtime_handle.clone().block_on(async move {
		let module = tangram_module::resolve(&compiler.handle, &referrer, &import)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					?referrer,
					%specifier,
					"failed to resolve specifier relative to the module"
				)
			})?;
		Ok(Serde(module))
	})
}

pub fn version(
	compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<String> {
	let (Serde(module),) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let version = compiler
			.get_module_version(&module)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the module version"))?;
		Ok(version.to_string())
	})
}

pub fn has_invalidated_resolutions(
	_compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	_args: (Serde<tg::module::Data>,),
) -> tg::Result<bool> {
	Ok(false)
}
