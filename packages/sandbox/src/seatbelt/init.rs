use {
	std::{os::unix::process::CommandExt as _, process::ExitCode},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Arg {
	pub serve: crate::serve::Arg,
}

pub fn run(arg: &Arg) -> tg::Result<ExitCode> {
	let error = crate::init::command(&arg.serve).exec();
	Err(tg::error!(!error, "failed to execute the sandbox server"))
}
