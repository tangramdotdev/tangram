use {tangram_client::prelude::*, usage_rs as usage};

pub mod token;

/// Work with authorization.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Token(self::token::Args),
}

pub fn run(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Token(args) => crate::token::run(args)?,
	}

	Ok(())
}
