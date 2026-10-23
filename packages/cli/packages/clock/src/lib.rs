use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod advance;
pub mod get;
pub mod set;

/// The component.
pub struct Component<'a>(&'a mut Cli);

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Advance(self::advance::Args),
	Get(self::get::Args),
	Set(self::set::Args),
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_clock(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Advance(args) => {
				self.command_clock_advance(args).await?;
			},
			Command::Get(args) => {
				self.command_clock_get(args).await?;
			},
			Command::Set(args) => {
				self.command_clock_set(args).await?;
			},
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
