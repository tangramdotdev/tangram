use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub serve: crate::serve::Arg,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	crate::init::serve(&arg.serve)
}
