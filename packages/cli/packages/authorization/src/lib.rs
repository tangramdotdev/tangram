use tangram_client::prelude::*;

pub mod token;

/// Work with authorization.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Token(self::token::Args),
}

pub fn run(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Token(args) => crate::token::run(args)?,
	}

	Ok(())
}
