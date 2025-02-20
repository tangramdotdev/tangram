use crate::Cli;
use tangram_client as tg;

pub mod build;
pub mod cancel;
pub mod children;
pub mod exec;
pub mod get;
pub mod log;
pub mod metadata;
pub mod output;
pub mod pull;
pub mod push;
pub mod put;
pub mod run;
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
	Build(self::build::Args),
	Cancel(self::cancel::Args),
	Children(self::children::Args),
	Get(self::get::Args),
	Log(self::log::Args),
	Metadata(self::metadata::Args),
	Output(self::output::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
	Run(self::run::Args),
	Spawn(self::spawn::Args),
	Status(self::status::Args),
	Wait(self::wait::Args),
}

impl Cli {
	pub async fn command_process(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Build(args) => {
				self.command_process_build(args).await?;
			},
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
			Command::Pull(args) => {
				self.command_process_pull(args).await?;
			},
			Command::Push(args) => {
				self.command_process_push(args).await?;
			},
			Command::Put(args) => {
				self.command_process_put(args).await?;
			},
			Command::Run(args) => {
				self.command_process_run(args).await?;
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
