use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod add;
pub mod list;
pub mod remove;

/// Manage organization members.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Add(self::add::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	#[command(alias = "delete", alias = "rm")]
	Remove(self::remove::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Add(args) => {
			crate::members::add::run(cli, args).await?;
		},
		Command::List(args) => {
			crate::members::list::run(cli, args).await?;
		},
		Command::Remove(args) => {
			crate::members::remove::run(cli, args).await?;
		},
	}
	Ok(())
}
