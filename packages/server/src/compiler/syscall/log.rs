use crate::compiler::Compiler;
use tangram_client as tg;

pub fn log(_compiler: &Compiler, _scope: &mut v8::HandleScope, args: (String,)) -> tg::Result<()> {
	let (string,) = args;
	tracing::info!("{string}");
	Ok(())
}
