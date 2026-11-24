use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod add;
pub mod list;
pub mod remove;

/// Manage group members.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	Add(self::add::Args),

	#[usage(alias = "ls")]
	List(self::list::Args),

	#[usage(alias = "delete", alias = "rm")]
	Remove(self::remove::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Add(args) => crate::members::add::run(cli, args).await?,
		Command::List(args) => crate::members::list::run(cli, args).await?,
		Command::Remove(args) => {
			crate::members::remove::run(cli, args).await?;
		},
	}
	Ok(())
}
