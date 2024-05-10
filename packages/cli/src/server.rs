use crate::Cli;
use tangram_client as tg;

pub mod clean;
pub mod health;
pub mod run;
pub mod start;
pub mod stop;

/// Manage the server.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Clean(self::clean::Args),
	Health(self::health::Args),
	Run(self::run::Args),
	Start(self::start::Args),
	Stop(self::stop::Args),
}

impl Cli {
	pub async fn command_server(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Clean(args) => {
				self.command_server_clean(args).await?;
			},
			Command::Health(args) => {
				self.command_server_health(args).await?;
			},
			Command::Run(args) => {
				self.command_server_run(args).await?;
			},
			Command::Start(args) => {
				self.command_server_start(args).await?;
			},
			Command::Stop(args) => {
				self.command_server_stop(args).await?;
			},
		}
		Ok(())
	}
}
