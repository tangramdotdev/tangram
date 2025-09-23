use {crate::compiler::Compiler, tangram_client as tg, tangram_v8::Serde};

pub fn list(
	compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	_args: v8::Local<'_, v8::Value>,
) -> tg::Result<Serde<Vec<tg::module::Data>>> {
	let modules = compiler
		.main_runtime_handle
		.clone()
		.block_on(async move { compiler.list_documents().await });
	Ok(Serde(modules))
}
