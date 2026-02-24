use {crate::Cli, tangram_client::prelude::*};

pub mod activate;
pub mod common;
pub mod completion;
pub mod deactivate;
pub mod directory;
pub mod integration;

#[derive(Clone, Copy, Debug, Eq, PartialEq, clap::ValueEnum)]
pub enum Kind {
	Bash,
	Fish,
	Nu,
	Zsh,
}

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

impl Cli {
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
