use crate::Cli;
use tangram_client as tg;

pub mod cancel;
pub mod children;
pub mod get;
pub mod log;
pub mod outcome;
pub mod output;
pub mod pull;
pub mod push;
pub mod put;
pub mod status;

/// Build a target or manage builds.
#[derive(Clone, Debug, clap::Args)]
#[command(args_conflicts_with_subcommands = true)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub args: crate::target::build::Args,

	#[command(subcommand)]
	pub command: Option<Command>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Cancel(self::cancel::Args),
	Children(self::children::Args),
	Get(self::get::Args),
	Log(self::log::Args),
	Outcome(self::outcome::Args),
	Output(self::output::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
	Status(self::status::Args),
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> tg::Result<()> {
		match args.command {
			None => {
				self.command_target_build(args.args).await?;
			},
			Some(Command::Cancel(args)) => {
				self.command_build_cancel(args).await?;
			},
			Some(Command::Children(args)) => {
				self.command_build_children(args).await?;
			},
			Some(Command::Get(args)) => {
				self.command_build_get(args).await?;
			},
			Some(Command::Log(args)) => {
				self.command_build_log(args).await?;
			},
			Some(Command::Outcome(args)) => {
				self.command_build_outcome(args).await?;
			},
			Some(Command::Output(args)) => {
				self.command_build_output(args).await?;
			},
			Some(Command::Pull(args)) => {
				self.command_build_pull(args).await?;
			},
			Some(Command::Push(args)) => {
				self.command_build_push(args).await?;
			},
			Some(Command::Put(args)) => {
				self.command_build_put(args).await?;
			},
			Some(Command::Status(args)) => {
				self.command_build_status(args).await?;
			},
		}
		Ok(())
	}
}
