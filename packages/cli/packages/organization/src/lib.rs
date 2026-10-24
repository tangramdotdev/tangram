use {tangram_cli_core::Cli, tangram_client::prelude::*};

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

pub async fn command_organization(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Billing(args) => crate::billing::command_organization_billing(cli, args).await?,
		Command::Create(args) => crate::create::command_organization_create(cli, args).await?,
		Command::Delete(args) => crate::delete::command_organization_delete(cli, args).await?,
		Command::Get(args) => crate::get::command_organization_get(cli, args).await?,
		Command::Members(args) => crate::members::command_organization_members(cli, args).await?,
		Command::Usage(args) => crate::usage::command_organization_usage(cli, args).await?,
	}
	Ok(())
}
