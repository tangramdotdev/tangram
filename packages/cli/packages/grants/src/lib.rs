use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod get;
pub mod grant;
pub mod list;
pub mod revoke;

/// Manage grants.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	#[command(alias = "ls")]
	List(self::list::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => crate::delete::run(cli, args).await?,
		Command::Get(args) => crate::get::run(cli, args).await?,
		Command::List(args) => crate::list::run(cli, args).await?,
	}

	Ok(())
}
