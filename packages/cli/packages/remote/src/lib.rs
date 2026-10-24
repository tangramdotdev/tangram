use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

/// Manage remotes.
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
	#[command(alias = "add")]
	Put(self::put::Args),
}

pub async fn command_remote(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => {
			crate::delete::command_remote_delete(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::command_remote_get(cli, args).await?;
		},
		Command::List(args) => {
			crate::list::command_remote_list(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::command_remote_put(cli, args).await?;
		},
	}
	Ok(())
}
