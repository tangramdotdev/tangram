use crate::Cli;
use tangram_client as tg;

pub mod cancel;
pub mod children;
pub mod get;
pub mod log;
pub mod metadata;
pub mod output;
pub mod put;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod wait;

/// Manage processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Cancel(self::cancel::Args),
	Children(self::children::Args),
	Get(self::get::Args),
	Log(self::log::Args),
	Metadata(self::metadata::Args),
	Output(self::output::Args),
	Put(self::put::Args),
	#[command(alias = "kill")]
	Signal(self::signal::Args),
	Spawn(self::spawn::Args),
	Status(self::status::Args),
	Wait(self::wait::Args),
}

impl Cli {
	pub async fn command_process(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Cancel(args) => {
				self.command_process_cancel(args).await?;
			},
			Command::Children(args) => {
				self.command_process_children(args).await?;
			},
			Command::Get(args) => {
				self.command_process_get(args).await?;
			},
			Command::Log(args) => {
				self.command_process_log(args).await?;
			},
			Command::Metadata(args) => {
				self.command_process_metadata(args).await?;
			},
			Command::Output(args) => {
				self.command_process_output(args).await?;
			},
			Command::Put(args) => {
				self.command_process_put(args).await?;
			},
			Command::Signal(args) => {
				self.command_process_signal(args).await?;
			},
			Command::Spawn(args) => {
				self.command_process_spawn(args).await?;
			},
			Command::Status(args) => {
				self.command_process_status(args).await?;
			},
			Command::Wait(args) => {
				self.command_process_wait(args).await?;
			},
		}
		Ok(())
	}
}
