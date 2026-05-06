use {
	crate::Cli,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub profile: PathBuf,

	#[arg(allow_hyphen_values = true, required = true, trailing_var_arg = true)]
	pub command: Vec<OsString>,
}

impl Args {
	fn into_arg(self) -> tangram_sandbox::seatbelt::run::Arg {
		tangram_sandbox::seatbelt::run::Arg {
			command: self.command,
			profile: self.profile,
		}
	}
}

impl Cli {
	pub fn command_sandbox_seatbelt_run(args: Args) -> tg::Result<std::process::ExitCode> {
		let arg = args.into_arg();
		tangram_sandbox::seatbelt::run::run(&arg)
	}
}
