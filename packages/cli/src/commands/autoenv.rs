use crate::Cli;
use futures::FutureExt;
use itertools::Itertools;
use std::path::PathBuf;
use tangram_error::{Result, WrapErr};

/// Manage autoenv paths.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	/// Add an autoenv path.
	Add(AddArgs),

	/// Get the autoenv path for a path.
	Get(GetArgs),

	/// List autoenv paths.
	List(ListArgs),

	/// Remove an autoenv path.
	Remove(RemoveArgs),
}

/// Add an autoenv path.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct AddArgs {
	pub path: Option<PathBuf>,
}

/// Get the autoenv path for a path.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct GetArgs {}

/// List autoenv paths.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct ListArgs {}

/// Remove an autoenv path.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct RemoveArgs {
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_autoenv(&self, args: Args) -> Result<()> {
		match args.command {
			Command::Add(args) => self.command_autoenv_add(args).boxed(),
			Command::Get(args) => self.command_autoenv_get(args).boxed(),
			Command::List(args) => self.command_autoenv_list(args).boxed(),
			Command::Remove(args) => self.command_autoenv_remove(args).boxed(),
		}
		.await?;
		Ok(())
	}

	async fn command_autoenv_add(&self, args: AddArgs) -> Result<()> {
		// Get the path.
		let mut path = std::env::current_dir().wrap_err("Failed to get the working directory.")?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}
		let path = tokio::fs::canonicalize(&path)
			.await
			.wrap_err("Failed to canonicalize the path.")?;

		// Get the config.
		let mut config = self.config().await?.unwrap_or_default();

		// Add the autoenv path.
		let mut autoenv = config.autoenv.unwrap_or_default();
		autoenv.paths.push(path);
		config.autoenv = Some(autoenv);

		// Save the config.
		self.save_config(config).await?;

		Ok(())
	}

	async fn command_autoenv_get(&self, _args: GetArgs) -> Result<()> {
		// Get the config.
		let config = self.config().await?.unwrap_or_default();

		// Get the working directory path.
		let working_directory_path =
			std::env::current_dir().wrap_err("Failed to get the working directory.")?;

		// Get the autoenv path for the working directory path.
		let Some(autoenv) = config.autoenv.as_ref() else {
			return Ok(());
		};
		let mut autoenv_paths = autoenv
			.paths
			.iter()
			.filter(|path| working_directory_path.starts_with(path))
			.collect_vec();
		autoenv_paths.sort_by_key(|path| path.components().count());
		autoenv_paths.reverse();
		let Some(autoenv_path) = autoenv_paths.first() else {
			return Ok(());
		};
		let autoenv_path = *autoenv_path;

		// Print the autoenv path.
		println!("{}", autoenv_path.display());

		Ok(())
	}

	async fn command_autoenv_list(&self, _args: ListArgs) -> Result<()> {
		// Get the config.
		let config = self.config().await?;

		// Get the autoenv paths.
		let autoenv_paths = config
			.as_ref()
			.and_then(|config| config.autoenv.as_ref())
			.map(|autoenv| autoenv.paths.clone())
			.unwrap_or_default();

		// If there are no autoenv paths, then print a message.
		if autoenv_paths.is_empty() {
			eprintln!("There are no autoenvs.");
		}

		// Print the autoenv paths.
		for path in autoenv_paths {
			let path = path.display();
			println!("{path}");
		}

		Ok(())
	}

	async fn command_autoenv_remove(&self, args: RemoveArgs) -> Result<()> {
		// Get the config.
		let mut config = self.config().await?.unwrap_or_default();

		// Get the path.
		let mut path = std::env::current_dir().wrap_err("Failed to get the working directory.")?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}
		let path = tokio::fs::canonicalize(&path)
			.await
			.wrap_err("Failed to canonicalize the path.")?;

		// Remove the autoenv path.
		if let Some(autoenv) = config.autoenv.as_mut() {
			if let Some(index) = autoenv.paths.iter().position(|p| *p == path) {
				autoenv.paths.remove(index);
			}
		}

		// Write the config.
		self.save_config(config).await?;

		Ok(())
	}
}
