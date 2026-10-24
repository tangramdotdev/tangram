use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod activate;
pub mod completion;
pub mod deactivate;
pub mod directory;
pub mod integration;
pub mod util;

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

pub async fn command_shell(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Activate(args) => {
			crate::activate::command_shell_activate(cli, args).await?;
		},
		Command::Completion(args) => {
			crate::completion::command_shell_completion(cli, args).await?;
		},
		Command::Deactivate(args) => {
			crate::deactivate::command_shell_deactivate(cli, args).await?;
		},
		Command::Directory(args) => {
			crate::directory::command_shell_directory(cli, args).await?;
		},
		Command::Integration(args) => {
			crate::integration::command_shell_integration(args).await?;
		},
	}
	Ok(())
}
