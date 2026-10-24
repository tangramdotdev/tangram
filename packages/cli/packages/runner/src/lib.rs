use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
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

	#[command(alias = "ls")]
	List(self::list::Args),

	Token(self::token::Args),
}

pub async fn command_runner(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Create(args) => crate::create::command_runner_create(cli, args).await?,
		Command::Delete(args) => crate::delete::command_runner_delete(cli, args).await?,
		Command::List(args) => crate::list::command_runner_list(cli, args).await?,
		Command::Token(args) => crate::token::command_runner_token(cli, args).await?,
	}

	Ok(())
}
