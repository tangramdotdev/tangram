use {std::process::ExitCode, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub struct Arg {
	pub linux: crate::linux::init::Arg,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	crate::linux::init::run(&arg.linux)
}
