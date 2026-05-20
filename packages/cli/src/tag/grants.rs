use {crate::Cli, tangram_client::prelude::*};

pub mod add;
pub mod delete;
pub mod list;

/// Manage tag grants.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Add(self::add::Args),

	#[command(alias = "remove", alias = "revoke", alias = "rm")]
	Delete(self::delete::Args),

	#[command(alias = "ls")]
	List(self::list::Args),
}

impl Cli {
	pub async fn command_tag_grants(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Add(args) => self.command_tag_grants_add(args).await?,
			Command::Delete(args) => self.command_tag_grants_delete(args).await?,
			Command::List(args) => self.command_tag_grants_list(args).await?,
		}
		Ok(())
	}
}
