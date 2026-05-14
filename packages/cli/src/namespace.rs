use {crate::Cli, tangram_client::prelude::*};

pub mod create;
pub mod delete;
pub mod get;
pub mod grants;

/// Manage namespaces.
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

	Grant(self::grants::add::Args),

	Grants(self::grants::Args),

	Revoke(self::grants::delete::Args),
}

impl Cli {
	pub async fn command_namespace(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Create(args) => {
				self.command_namespace_create(args).await?;
			},
			Command::Delete(args) => {
				self.command_namespace_delete(args).await?;
			},
			Command::Get(args) => {
				self.command_namespace_get(args).await?;
			},
			Command::Grant(args) => {
				self.command_namespace_grants_add(args).await?;
			},
			Command::Grants(args) => {
				self.command_namespace_grants(args).await?;
			},
			Command::Revoke(args) => {
				self.command_namespace_grants_delete(args).await?;
			},
		}
		Ok(())
	}
}
