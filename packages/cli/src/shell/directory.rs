use {crate::Cli, tangram_client::prelude::*};

pub mod delete;
pub mod list;
pub mod put;
pub mod update;

/// Manage shell directories.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	#[command(name = "remove", alias = "rm")]
	Delete(self::delete::Args),
	#[command(alias = "ls")]
	List(self::list::Args),
	#[command(name = "add")]
	Put(self::put::Args),
	#[command(hide = true)]
	Update(self::update::Args),
}

impl Cli {
	pub async fn command_shell_directory(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Delete(args) => {
				self.command_shell_directory_delete(args).await?;
			},
			Command::List(args) => {
				self.command_shell_directory_list(args).await?;
			},
			Command::Put(args) => {
				self.command_shell_directory_put(args).await?;
			},
			Command::Update(args) => {
				self.command_shell_directory_update(args).await?;
			},
		}
		Ok(())
	}
}
