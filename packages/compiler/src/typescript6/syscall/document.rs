use {crate::Compiler, tangram_client::prelude::*, tangram_v8::Serde};

pub fn list(
	compiler: &Compiler,
	_scope: &mut v8::PinScope<'_, '_>,
	_args: v8::Local<'_, v8::Value>,
) -> tg::Result<Serde<Vec<tg::module::Data>>> {
	let modules = compiler
		.main_runtime_handle
		.clone()
		.block_on(async move { compiler.list_documents().await });
	Ok(Serde(modules))
}
