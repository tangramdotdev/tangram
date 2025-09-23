use {crate::Cli, tangram_client as tg};

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

impl Cli {
	pub async fn command_server(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Restart(args) => {
				self.command_server_restart(args).await?;
			},
			Command::Run(args) => {
				self.command_server_run(args).await?;
			},
			Command::Start(args) => {
				self.command_server_start(args).await?;
			},
			Command::Status(args) => {
				self.command_server_status(args).await?;
			},
			Command::Stop(args) => {
				self.command_server_stop(args).await?;
			},
		}
		Ok(())
	}
}
