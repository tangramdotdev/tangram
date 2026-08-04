use {
	crate::Cli,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
};

mod archive;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod util;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Archive(archive::Args),
	Checksum(checksum::Args),
	Compress(compress::Args),
	Decompress(decompress::Args),
	Download(download::Args),
	Extract(extract::Args),
}

impl Command {
	fn output(&self) -> Option<PathBuf> {
		let (named, positional) = match self {
			Self::Archive(args) => (&args.output_named, &args.output_positional),
			Self::Checksum(args) => (&args.output_named, &args.output_positional),
			Self::Compress(args) => (&args.output_named, &args.output_positional),
			Self::Decompress(args) => (&args.output_named, &args.output_positional),
			Self::Download(args) => (&args.output_named, &args.output_positional),
			Self::Extract(args) => (&args.output_named, &args.output_positional),
		};
		util::resolve_path(named.clone(), positional.clone())
	}
}

impl Cli {
	pub async fn command_builtin(&mut self, args: Args) -> tg::Result<()> {
		let output = args.command.output();
		let result = match args.command {
			Command::Archive(args) => archive::run(args).await,
			Command::Checksum(args) => checksum::run(args).await,
			Command::Compress(args) => compress::run(args).await,
			Command::Decompress(args) => decompress::run(args).await,
			Command::Download(args) => download::run(args).await,
			Command::Extract(args) => extract::run(args).await,
		};

		if let Err(error) = &result {
			Self::write_builtin_error(output.as_deref(), error).await?;
		}

		result
	}

	async fn write_builtin_error(output: Option<&Path>, error: &tg::Error) -> tg::Result<()> {
		let path = std::env::var_os("TANGRAM_OUTPUT").map(PathBuf::from);
		let (Some(path), Some(output)) = (path, output) else {
			return Ok(());
		};
		if path != output {
			return Ok(());
		}

		// Get the error data.
		let Some(data) = error
			.state()
			.object()
			.map(|object| object.unwrap_error_ref().to_data())
		else {
			return Ok(());
		};

		// Create the output if the builtin did not, because the xattr requires it.
		if tokio::fs::symlink_metadata(&path).await.is_err() {
			tokio::fs::write(&path, b"").await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to write the output"),
			)?;
		}

		// Write the error xattr.
		let json = serde_json::to_vec(&data)
			.map_err(|error| tg::error!(!error, "failed to serialize the error"))?;
		xattr::set(&path, "user.tangram.error", &json)
			.map_err(|error| tg::error!(!error, "failed to write the error xattr"))?;

		Ok(())
	}
}
