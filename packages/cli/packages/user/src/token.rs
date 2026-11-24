use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod list;

/// Manage user tokens.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	Create(self::create::Args),

	#[usage(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	#[usage(alias = "ls")]
	List(self::list::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Create(args) => crate::token::create::run(cli, args).await?,
		Command::Delete(args) => crate::token::delete::run(cli, args).await?,
		Command::Get(args) => crate::token::get::run(cli, args).await?,
		Command::List(args) => crate::token::list::run(cli, args).await?,
	}

	Ok(())
}
