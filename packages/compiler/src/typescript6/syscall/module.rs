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
	let invalidated = compiler
		.main_runtime_handle
		.block_on(compiler.has_invalidated_module_resolutions(&module));
	Ok(invalidated)
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
	let module = compiler
		.main_runtime_handle
		.block_on(compiler.resolve_module(&referrer, &specifier, attributes))?;
	Ok(Serde(module))
}

pub fn validate_resolutions(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	args: (Serde<tg::module::Data>,),
) -> tg::Result<()> {
	let (Serde(module),) = args;
	compiler
		.main_runtime_handle
		.block_on(compiler.validate_module_resolutions(&module));
	Ok(())
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
