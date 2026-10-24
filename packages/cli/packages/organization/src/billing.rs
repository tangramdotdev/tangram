use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod manage;

/// Manage organization billing.
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

pub async fn command_organization_billing(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Manage(args) => {
			crate::billing::manage::command_organization_billing_manage(cli, args).await?;
		},
	}
	Ok(())
}
