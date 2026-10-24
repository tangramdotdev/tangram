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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => {
			crate::delete::run(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::run(cli, args).await?;
		},
		Command::List(args) => {
			crate::list::run(cli, args).await?;
		},
	}
	Ok(())
}
