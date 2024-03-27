use std::path::PathBuf;

use tangram_client as tg;

use crate::Cli;
use tg::Handle;

/// Manage watches.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Add(AddArgs),
	Remove(RemoveArgs),
	List(ListArgs),
}

#[derive(Debug, clap::Args)]
pub struct AddArgs {
	pub path: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct RemoveArgs {
	pub path: PathBuf,
}

#[derive(Debug, clap::Args)]

pub struct ListArgs;

impl Cli {
	pub async fn command_watch(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Add(args) => self.command_watch_add(args).await,
			Command::List(args) => self.command_watch_list(args).await,
			Command::Remove(args) => self.command_watch_remove(args).await,
		}
	}

	async fn command_watch_add(&self, args: AddArgs) -> tg::Result<()> {
		let client = &self.client().await?;
		let path = args
			.path
			.canonicalize()
			.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?;
		let path = path.try_into()?;
		tg::Artifact::check_in(client, &path, true)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to add watch"))?;
		Ok(())
	}

	async fn command_watch_list(&self, _args: ListArgs) -> tg::Result<()> {
		let client = self.client().await?;
		let paths = client
			.list_watches()
			.await
			.map_err(|source| tg::error!(!source, "failed to get watches"))?;
		for path in paths {
			eprintln!("{path}");
		}
		Ok(())
	}

	async fn command_watch_remove(&self, args: RemoveArgs) -> tg::Result<()> {
		let client = self.client().await?;
		let path = args.path.try_into()?;
		client
			.remove_watch(&path)
			.await
			.map_err(|source| tg::error!(!source, %path, "failed to remove watch"))?;
		Ok(())
	}
}
