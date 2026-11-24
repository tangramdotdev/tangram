use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod delete;
pub mod list;
pub mod put;
pub mod update;

/// Manage shell directories.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	#[usage(alias = "rm", name = "remove")]
	Delete(self::delete::Args),
	#[usage(alias = "ls")]
	List(self::list::Args),
	#[usage(name = "add")]
	Put(self::put::Args),
	#[usage(hide = true)]
	Update(self::update::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Delete(args) => {
			crate::directory::delete::run(cli, args).await?;
		},
		Command::List(args) => {
			crate::directory::list::run(cli, args).await?;
		},
		Command::Put(args) => {
			crate::directory::put::run(cli, args).await?;
		},
		Command::Update(args) => {
			crate::directory::update::run(cli, args).await?;
		},
	}
	Ok(())
}
