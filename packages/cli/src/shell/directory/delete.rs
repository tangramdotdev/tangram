use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Delete a shell directory.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_shell_directory_delete(&mut self, args: Args) -> tg::Result<()> {
		let key = std::fs::canonicalize(&args.path).map_err(|source| {
			tg::error!(
				!source,
				path = %args.path.display(),
				"failed to canonicalize the directory path"
			)
		})?;
		if !key.is_dir() {
			return Err(tg::error!(path = %key.display(), "the path is not a directory"));
		}
		let key = key.to_str().map(ToOwned::to_owned).ok_or_else(|| {
			tg::error!(
				path = %key.display(),
				"the directory path is not valid UTF-8"
			)
		})?;
		let mut config = self.read_config()?;
		let shell = config.shell.as_mut().ok_or_else(|| {
			tg::error!(
				path = %args.path.display(),
				"the shell directory is not configured"
			)
		})?;
		let removed = shell.directories.remove(&key).is_some();
		let directories_empty = shell.directories.is_empty();
		if !removed {
			return Err(tg::error!(
				path = %args.path.display(),
				"the shell directory is not configured"
			));
		}
		if directories_empty {
			config.shell = None;
		}
		self.write_config(&config)?;
		Ok(())
	}
}
