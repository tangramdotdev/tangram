use crate::Cli;
use tangram_client as tg;

pub mod checkin;
pub mod checkout;

/// Manage objects.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Checkin(self::checkin::Args),
	Checkout(self::checkout::Args),
	Push(crate::object::push::Args),
	Pull(crate::object::pull::Args),
}

impl Cli {
	pub async fn command_artifact(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Checkin(args) => {
				self.command_artifact_checkin(args).await?;
			},
			Command::Checkout(args) => {
				self.command_artifact_checkout(args).await?;
			},
			Command::Push(args) => {
				self.command_object_push(args).await?;
			},
			Command::Pull(args) => {
				self.command_object_pull(args).await?;
			},
		}
		Ok(())
	}
}
