use crate::compiler::Compiler;
use tangram_client as tg;
use tangram_v8::Serde;

pub fn list(
	compiler: &Compiler,
	_scope: &mut v8::HandleScope,
	_args: (),
) -> tg::Result<Serde<Vec<tg::module::Data>>> {
	let modules = compiler
		.main_runtime_handle
		.clone()
		.block_on(async move { compiler.list_documents().await });
	Ok(Serde(modules))
}
