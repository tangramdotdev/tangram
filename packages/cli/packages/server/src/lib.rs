use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod config;
pub mod restart;
pub mod run;
pub mod start;
pub mod status;
pub mod stop;

/// Manage the server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Restart(self::restart::Args),
	Run(self::run::Args),
	Start(self::start::Args),
	Status(self::status::Args),
	Stop(self::stop::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Restart(args) => {
			self::restart::run(cli, args).await?;
		},
		Command::Run(args) => {
			Box::pin(self::run::run(cli, args)).await?;
		},
		Command::Start(args) => {
			self::start::run(cli, args).await?;
		},
		Command::Status(args) => {
			self::status::run(cli, args).await?;
		},
		Command::Stop(args) => {
			self::stop::run(cli, args).await?;
		},
	}
	Ok(())
}
