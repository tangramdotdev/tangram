use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod advance;
pub mod get;
pub mod set;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Advance(self::advance::Args),
	Get(self::get::Args),
	Set(self::set::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Advance(args) => {
			crate::advance::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::Set(args) => {
			crate::set::run(cli, args).await?;
		},
	}
	Ok(())
}
