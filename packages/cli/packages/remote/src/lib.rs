use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

/// Manage remotes.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	#[usage(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	Get(self::get::Args),
	#[usage(alias = "ls")]
	List(self::list::Args),
	#[usage(alias = "add")]
	Put(self::put::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => {
			crate::delete::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::List(args) => {
			crate::list::run(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::run(cli, args).await?;
		},
	}
	Ok(())
}
