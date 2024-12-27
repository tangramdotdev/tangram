use crate::Cli;
use tangram_client as tg;

pub mod get;
pub mod import;
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
	Get(self::get::Args),
	Import(self::import::Args),
	Metadata(self::metadata::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Get(args) => {
				self.command_object_get(args).await?;
			},
			Command::Import(args) => {
				self.command_object_import(args).await?;
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
