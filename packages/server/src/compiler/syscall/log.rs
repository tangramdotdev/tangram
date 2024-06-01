use crate::compiler::Compiler;
use tangram_client as tg;

pub fn log(_scope: &mut v8::HandleScope, _compiler: Compiler, args: (String,)) -> tg::Result<()> {
	let (string,) = args;
	tracing::info!("{string}");
	Ok(())
}
