use crate::compiler::Compiler;
use tangram_client as tg;

pub fn list(
	_scope: &mut v8::HandleScope,
	compiler: Compiler,
	_args: (),
) -> tg::Result<Vec<tg::Module>> {
	compiler
		.main_runtime_handle
		.clone()
		.block_on(async move { Ok(compiler.list_documents().await) })
}
