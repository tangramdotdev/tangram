use {crate::Cli, tangram_client::prelude::*};

pub mod billing;
pub mod create;
pub mod delete;
pub mod get;
pub mod members;
pub mod usage;

/// Manage organizations.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Billing(self::billing::Args),

	#[command(alias = "add")]
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	Members(self::members::Args),

	Usage(self::usage::Args),
}

impl Cli {
	pub async fn command_organization(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Billing(args) => self.command_organization_billing(args).await?,
			Command::Create(args) => self.command_organization_create(args).await?,
			Command::Delete(args) => self.command_organization_delete(args).await?,
			Command::Get(args) => self.command_organization_get(args).await?,
			Command::Members(args) => self.command_organization_members(args).await?,
			Command::Usage(args) => self.command_organization_usage(args).await?,
		}
		Ok(())
	}
}
