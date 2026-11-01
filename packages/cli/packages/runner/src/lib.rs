use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod list;
pub mod token;

/// Manage runners.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	Token(self::token::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Create(args) => crate::create::run(cli, args).await?,
		Command::Delete(args) => crate::delete::run(cli, args).await?,
		Command::Get(args) => crate::get::run(cli, args).await?,
		Command::List(args) => crate::list::run(cli, args).await?,
		Command::Token(args) => crate::token::run(cli, args).await?,
	}

	Ok(())
}
