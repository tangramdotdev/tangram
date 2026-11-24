use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod delete;
pub mod get;
pub mod grant;
pub mod list;
pub mod revoke;

/// Manage grants.
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
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => crate::delete::run(cli, args).await?,
		Command::Get(args) => crate::get::run(cli, args).await?,
		Command::List(args) => crate::list::run(cli, args).await?,
	}

	Ok(())
}
