use crate::Cli;
use tangram_client as tg;

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

/// Manage remotes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Delete(self::delete::Args),
	Get(self::get::Args),
	List(self::list::Args),
	Put(self::put::Args),
}

impl Cli {
	pub async fn command_remote(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Delete(args) => {
				self.command_remote_delete(args).await?;
			},
			Command::Get(args) => {
				self.command_remote_get(args).await?;
			},
			Command::List(args) => {
				self.command_remote_list(args).await?;
			},
			Command::Put(args) => {
				self.command_remote_put(args).await?;
			},
		}
		Ok(())
	}
}
