use crate::Cli;
use tangram_client as tg;

pub mod update;

/// Manage Tangram.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Update(self::update::Args),
}

impl Cli {
	pub async fn command_tangram(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Update(args) => {
				self.command_tangram_update(args).await?;
			},
		}
		Ok(())
	}
}
