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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Billing(args) => crate::billing::run(cli, args).await?,
		Command::Create(args) => crate::create::run(cli, args).await?,
		Command::Delete(args) => crate::delete::run(cli, args).await?,
		Command::Get(args) => crate::get::run(cli, args).await?,
		Command::Members(args) => crate::members::run(cli, args).await?,
		Command::Usage(args) => crate::usage::run(cli, args).await?,
	}
	Ok(())
}
