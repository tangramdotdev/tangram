use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Delete a shell directory.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub path: PathBuf,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let key = std::fs::canonicalize(&args.path).map_err(|error| {
		tg::error!(
			!error,
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
	let mut config = cli.read_config_for_write()?;
	let directories = config
		.shell
		.as_mut()
		.and_then(|shell| shell.directories.as_mut())
		.ok_or_else(|| {
			tg::error!(
				path = %args.path.display(),
				"the shell directory is not configured"
			)
		})?;
	let removed = directories.remove(&key).is_some();
	let directories_empty = directories.is_empty();
	if !removed {
		return Err(tg::error!(
			path = %args.path.display(),
			"the shell directory is not configured"
		));
	}
	if directories_empty {
		config.shell = None;
	}
	cli.write_config(&config)?;
	Ok(())
}
