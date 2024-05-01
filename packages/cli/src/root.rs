use crate::Cli;
use tangram_client as tg;

pub mod add;
pub mod get;
pub mod list;
pub mod remove;

/// Manage roots.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Add(self::add::Args),
	Get(self::get::Args),
	List(self::list::Args),
	Remove(self::remove::Args),
}

impl Cli {
	pub async fn command_root(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Add(args) => {
				self.command_root_add(args).await?;
			},
			Command::Get(args) => {
				self.command_root_get(args).await?;
			},
			Command::List(args) => {
				self.command_root_list(args).await?;
			},
			Command::Remove(args) => {
				self.command_root_remove(args).await?;
			},
		}
		Ok(())
	}
}
