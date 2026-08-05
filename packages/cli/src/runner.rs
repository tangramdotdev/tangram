use {crate::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod list;
pub mod token;

/// Manage runners.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	Token(self::token::Args),
}

impl Cli {
	pub async fn command_runner(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => self.command_runner_create(args).await?,
			Command::Delete(args) => self.command_runner_delete(args).await?,
			Command::List(args) => self.command_runner_list(args).await?,
			Command::Token(args) => self.command_runner_token(args).await?,
		}

		Ok(())
	}
}
