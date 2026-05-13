use {crate::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod grants;
pub mod list;
pub mod member;

/// Manage groups.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(alias = "add")]
	Create(self::create::Args),

	#[command(alias = "remove", alias = "rm")]
	Delete(self::delete::Args),

	Get(self::get::Args),

	Grants(self::grants::Args),

	#[command(alias = "ls")]
	List(self::list::Args),

	#[command(alias = "members")]
	Member(self::member::Args),
}

impl Cli {
	pub async fn command_group(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => self.command_group_create(args).await?,
			Command::Delete(args) => self.command_group_delete(args).await?,
			Command::Get(args) => self.command_group_get(args).await?,
			Command::Grants(args) => self.command_group_grants(args).await?,
			Command::List(args) => self.command_group_list(args).await?,
			Command::Member(args) => self.command_group_member(args).await?,
		}
		Ok(())
	}
}
