use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod members;

/// Manage groups.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	#[usage(alias = "add")]
	Create(self::create::Args),

	#[usage(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	Members(self::members::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Create(args) => crate::create::run(cli, args).await?,
		Command::Delete(args) => crate::delete::run(cli, args).await?,
		Command::Get(args) => crate::get::run(cli, args).await?,
		Command::Members(args) => crate::members::run(cli, args).await?,
	}
	Ok(())
}
