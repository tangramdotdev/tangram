use tangram_client::prelude::*;

pub mod decode;
pub mod encode;

/// Work with authorization tokens.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	Decode(self::decode::Args),
	Encode(self::encode::Args),
}

pub fn run(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Decode(args) => crate::token::decode::run(args)?,
		Command::Encode(args) => crate::token::encode::run(args)?,
	}

	Ok(())
}
