use {crate::Cli, tangram_client::prelude::*};

pub mod add;
pub mod list;
pub mod remove;

/// Manage organization members.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "add")]
	Add(self::add::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	#[command(alias = "remove", alias = "rm")]
	Remove(self::remove::Args),
}

impl Cli {
	pub async fn command_organization_members(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Add(args) => self.command_organization_members_add(args).await?,
			Command::List(args) => self.command_organization_members_list(args).await?,
			Command::Remove(args) => self.command_organization_members_remove(args).await?,
		}
		Ok(())
	}
}
