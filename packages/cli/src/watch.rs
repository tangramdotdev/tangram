use {crate::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod list;

/// Manage watches.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Delete(self::delete::Args),
	List(self::list::Args),
}

impl Cli {
	pub async fn command_watch(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Delete(args) => {
				self.command_watch_delete(args).await?;
			},
			Command::List(args) => {
				self.command_watch_list(args).await?;
			},
		}
		Ok(())
	}
}
