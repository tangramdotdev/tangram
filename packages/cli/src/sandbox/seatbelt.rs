use {
	crate::Cli,
	std::{ffi::OsString, path::PathBuf},
	tangram_client::prelude::*,
};

/// Run a macOS seatbelt sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub profile: PathBuf,

	#[arg(allow_hyphen_values = true, required = true, trailing_var_arg = true)]
	pub command: Vec<OsString>,
}

impl Args {
	fn into_arg(self) -> tangram_sandbox::seatbelt::Arg {
		tangram_sandbox::seatbelt::Arg {
			command: self.command,
			profile: self.profile,
		}
	}
}

impl Cli {
	#[must_use]
	pub fn command_sandbox_seatbelt(args: Args) -> std::process::ExitCode {
		let arg = args.into_arg();
		match tangram_sandbox::seatbelt::run(&arg) {
			Ok(code) => code,
			Err(error) => {
				Cli::print_error_basic(tg::Referent::with_item(error));
				std::process::ExitCode::FAILURE
			},
		}
	}
}
