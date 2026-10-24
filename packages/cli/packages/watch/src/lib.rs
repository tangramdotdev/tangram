use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod list;
pub mod touch;

/// Manage watches.
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
	#[command(alias = "ls")]
	List(self::list::Args),
	Touch(self::touch::Args),
}

pub async fn command_watch(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => {
			crate::delete::command_watch_delete(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::command_watch_touch(cli, args).await?;
		},
		Command::List(args) => {
			crate::list::command_watch_list(cli, args).await?;
		},
	}
	Ok(())
}
