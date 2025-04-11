use crate::Cli;
use tangram_client as tg;

pub mod children;
pub mod get;
pub mod metadata;
pub mod put;

/// Manage objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Children(self::children::Args),
	Get(self::get::Args),
	Metadata(self::metadata::Args),
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_object(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Children(args) => {
				self.command_object_children(args).await?;
			},
			Command::Get(args) => {
				self.command_object_get(args).await?;
			},
			Command::Metadata(args) => {
				self.command_object_metadata(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
		}
		Ok(())
	}
}
