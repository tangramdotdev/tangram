use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod list;
pub mod touch;

/// Manage watches.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	#[usage(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),
	#[usage(alias = "ls")]
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
