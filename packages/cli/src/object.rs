use crate::Cli;
use tangram_client as tg;

pub mod get;
pub mod pull;
pub mod push;
pub mod put;
pub mod tree;

/// Manage objects.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Get(self::get::Args),
	Pull(self::pull::Args),
	Push(self::push::Args),
	Put(self::put::Args),
	Tree(self::tree::Args),
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Get(args) => {
				self.command_object_get(args).await?;
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
			Command::Tree(args) => {
				self.command_object_tree(args).await?;
			},
		}
		Ok(())
	}
}
