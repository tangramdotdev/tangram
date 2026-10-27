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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Activate(args) => {
			crate::activate::run(cli, args).await?;
		},
		Command::Completion(args) => {
			crate::completion::run(cli, args).await?;
		},
		Command::Deactivate(args) => {
			crate::deactivate::run(cli, args).await?;
		},
		Command::Directory(args) => {
			crate::directory::run(cli, args).await?;
		},
		Command::Integration(args) => {
			crate::integration::run(args).await?;
		},
	}
	Ok(())
}
