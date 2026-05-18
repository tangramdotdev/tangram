use {crate::Cli, tangram_client::prelude::*};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub fn command_sandbox_vm_init(_args: Args) -> tg::Result<std::process::ExitCode> {
		tangram_sandbox::vm::init::run()
	}
}
