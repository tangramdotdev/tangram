use crate::Cli;
use tangram_client as tg;

pub mod children;
pub mod get;
pub mod metadata;
pub mod pull;
pub mod push;
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
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> tg::Result<()> {
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
			Command::Pull(args) => {
				self.command_object_pull(args).await?;
			},
			Command::Push(args) => {
				self.command_object_push(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
		}
		Ok(())
	}
}
