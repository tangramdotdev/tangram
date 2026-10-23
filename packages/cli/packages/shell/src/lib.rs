use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod activate;
pub mod completion;
pub mod deactivate;
pub mod directory;
pub mod integration;
pub mod util;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Manage shell integration.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Activate(self::activate::Args),
	Completion(self::completion::Args),
	Deactivate(self::deactivate::Args),
	Directory(self::directory::Args),
	Integration(self::integration::Args),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum Kind {
	Bash,
	Fish,
	Nu,
	Zsh,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_shell(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Activate(args) => {
				self.command_shell_activate(args).await?;
			},
			Command::Completion(args) => {
				self.command_shell_completion(args).await?;
			},
			Command::Deactivate(args) => {
				self.command_shell_deactivate(args).await?;
			},
			Command::Directory(args) => {
				self.command_shell_directory(args).await?;
			},
			Command::Integration(args) => {
				self.command_shell_integration(args).await?;
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
