use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod list;
pub mod token;

/// The component.
pub struct Component<'a>(&'a mut Cli);

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

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
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

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
