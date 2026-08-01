use {crate::Cli, tangram_client::prelude::*};

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

impl Cli {
	pub async fn command_organization_billing(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Manage(args) => self.command_organization_billing_manage(args).await?,
		}
		Ok(())
	}
}
