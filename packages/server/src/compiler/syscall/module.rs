use crate::compiler::Compiler;
use std::collections::BTreeMap;
use tangram_client as tg;

pub fn load(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::module::Data,),
) -> tg::Result<String> {
	let (module,) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let text = compiler
			.load_module(&module)
			.await
			.map_err(|source| tg::error!(!source, ?module, "failed to load the module"))?;
		Ok(text)
	})
}

pub fn resolve(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::module::Data, String, Option<BTreeMap<String, String>>),
) -> tg::Result<tg::module::Data> {
	let (referrer, specifier, attributes) = args;
	let import = tg::module::Import::with_specifier_and_attributes(&specifier, attributes)
		.map_err(|source| tg::error!(!source, "failed to create the import"))?;
	compiler.main_runtime_handle.clone().block_on(async move {
		let module = compiler
			.resolve_module(&referrer, &import)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					?referrer,
					%specifier,
					"failed to resolve specifier relative to the module"
				)
			})?;
		Ok(module)
	})
}

pub fn version(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::module::Data,),
) -> tg::Result<String> {
	let (module,) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let version = compiler
			.get_module_version(&module)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the module version"))?;
		Ok(version.to_string())
	})
}

pub fn has_invalidated_resolutions(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::module::Data,),
) -> tg::Result<bool> {
	let (module,) = args;
	let Some(document) = compiler.documents.get(&module) else {
		return Ok(false);
	};
	Ok(!document.dirty)
}
