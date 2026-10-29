use tangram_client::prelude::*;

pub mod decode;

/// Work with authorization tokens.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Decode(self::decode::Args),
}

pub fn run(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Decode(args) => crate::token::decode::run(args)?,
	}

	Ok(())
}
