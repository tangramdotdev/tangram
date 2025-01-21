use crate::Cli;
use tangram_client as tg;

pub mod cancel;
pub mod children;
pub mod get;
pub mod log;
pub mod output;
pub mod pull;
pub mod push;
pub mod put;
pub mod status;

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
	Output(self::output::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
	Status(self::status::Args),
}

impl Cli {
	pub async fn command_process(&self, args: Args) -> tg::Result<()> {
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
			Command::Output(args) => {
				self.command_process_output(args).await?;
			},
			Command::Pull(args) => {
				self.command_process_pull(args).await?;
			},
			Command::Push(args) => {
				self.command_process_push(args).await?;
			},
			Command::Put(args) => {
				self.command_process_put(args).await?;
			},
			Command::Status(args) => {
				self.command_process_status(args).await?;
			},
		}
		Ok(())
	}
}
