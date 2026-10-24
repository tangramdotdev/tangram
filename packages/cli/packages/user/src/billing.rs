use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod manage;

/// Manage the user's billing.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Manage(self::manage::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Manage(args) => {
			crate::billing::manage::run(cli, args).await?;
		},
	}
	Ok(())
}
