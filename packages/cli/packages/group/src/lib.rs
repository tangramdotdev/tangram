use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod members;

/// Manage groups.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "add")]
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	Members(self::members::Args),
}

pub async fn command_group(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Create(args) => crate::create::command_group_create(cli, args).await?,
		Command::Delete(args) => crate::delete::command_group_delete(cli, args).await?,
		Command::Get(args) => crate::get::command_group_get(cli, args).await?,
		Command::Members(args) => crate::members::command_group_members(cli, args).await?,
	}
	Ok(())
}
