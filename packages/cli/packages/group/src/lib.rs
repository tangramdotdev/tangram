use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod members;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Manage groups.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "add")]
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	Members(self::members::Args),
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_group(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => self.command_group_create(args).await?,
			Command::Delete(args) => self.command_group_delete(args).await?,
			Command::Get(args) => self.command_group_get(args).await?,
			Command::Members(args) => self.command_group_members(args).await?,
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
