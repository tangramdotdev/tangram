use crate::Cli;
use tangram_client as tg;

pub mod build;
pub mod run;

/// Manage targets.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Build(self::build::Args),
	Run(self::run::Args),
}

impl Cli {
	pub async fn command_command(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Build(args) => {
				self.command_command_build(args).await?;
			},
			Command::Run(args) => {
				self.command_command_run(args).await?;
			},
		}
		Ok(())
	}
}
