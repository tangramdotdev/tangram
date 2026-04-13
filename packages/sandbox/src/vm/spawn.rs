use {crate::serve, tangram_client::prelude::*};

pub(crate) fn spawn(arg: &crate::Arg, serve_arg: &serve::Arg) -> tg::Result<tokio::process::Child> {
	let _ = arg;
	let _ = serve_arg;
	Err(tg::error!("vm isolation is not yet implemented"))
}
