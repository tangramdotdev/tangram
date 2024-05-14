use crate::compiler::Compiler;
use std::collections::BTreeMap;
use tangram_client as tg;

pub fn load(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::Module,),
) -> tg::Result<String> {
	let (module,) = args;
	compiler.main_runtime_handle.clone().block_on(async move {
		let text = compiler
			.load_module(&module)
			.await
			.map_err(|source| tg::error!(!source, %module, "failed to load the module"))?;
		Ok(text)
	})
}

pub fn resolve(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::Module, String, Option<BTreeMap<String, String>>),
) -> tg::Result<tg::Module> {
	let (module, specifier, attributes) = args;
	let import = tg::Import::with_specifier_and_attributes(&specifier, attributes.as_ref())
		.map_err(|source| tg::error!(!source, "failed to create the import"))?;
	compiler.main_runtime_handle.clone().block_on(async move {
		let module = compiler
			.resolve_module(&module, &import)
			.await
			.map_err(|error| {
				tg::error!(
					source = error,
					%specifier,
					%module,
					"failed to resolve specifier relative to the module"
				)
			})?;
		Ok(module)
	})
}

pub fn version(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	args: (tg::Module,),
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
