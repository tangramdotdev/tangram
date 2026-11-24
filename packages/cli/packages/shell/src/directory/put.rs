use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Put a shell directory.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub path: PathBuf,

	pub export: String,
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
	let shell = config.shell.get_or_insert_default();
	let directory = tangram_cli_core::config::ShellDirectory {
		export: args.export,
	};
	shell
		.directories
		.get_or_insert_default()
		.insert(key, directory);
	cli.write_config(&config)?;
	Ok(())
}
