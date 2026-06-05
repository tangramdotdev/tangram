use {crate::Cli, tangram_client::prelude::*};

pub mod add;
pub mod list;
pub mod remove;

/// Manage group members.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Add(self::add::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	#[command(alias = "delete", alias = "rm")]
	Remove(self::remove::Args),
}

impl Cli {
	pub async fn command_group_members(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Add(args) => self.command_group_members_add(args).await?,
			Command::List(args) => self.command_group_members_list(args).await?,
			Command::Remove(args) => self.command_group_members_remove(args).await?,
		}
		Ok(())
	}
}
