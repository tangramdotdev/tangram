use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod list;
pub mod put;
pub mod update;

/// Manage shell directories.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "rm", name = "remove")]
	Delete(self::delete::Args),
	#[command(alias = "ls")]
	List(self::list::Args),
	#[command(name = "add")]
	Put(self::put::Args),
	#[command(hide = true)]
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
