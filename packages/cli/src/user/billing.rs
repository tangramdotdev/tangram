use {crate::Cli, tangram_client::prelude::*};

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

impl Cli {
	pub async fn command_user_billing(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Manage(args) => self.command_user_billing_manage(args).await?,
		}
		Ok(())
	}
}
