use {crate::Compiler, tangram_client::prelude::*};

pub fn log(_compiler: &Compiler, _scope: &mut v8::HandleScope, args: (String,)) -> tg::Result<()> {
	let (string,) = args;
	tracing::info!("{string}");
	Ok(())
}
