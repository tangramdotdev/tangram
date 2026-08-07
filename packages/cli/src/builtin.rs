use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

mod archive;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod progress;
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

impl Cli {
	pub async fn command_builtin(&mut self, args: Args) -> tg::Result<()> {
		let (named, positional) = match &args.command {
			Command::Archive(args) => (&args.output_named, &args.output_positional),
			Command::Checksum(args) => (&args.output_named, &args.output_positional),
			Command::Compress(args) => (&args.output_named, &args.output_positional),
			Command::Decompress(args) => (&args.output_named, &args.output_positional),
			Command::Download(args) => (&args.output_named, &args.output_positional),
			Command::Extract(args) => (&args.output_named, &args.output_positional),
		};
		let output_path = util::resolve_path(named.clone(), positional.clone());

		let result = match args.command {
			Command::Archive(args) => archive::run(args).await,
			Command::Checksum(args) => checksum::run(args).await,
			Command::Compress(args) => compress::run(args).await,
			Command::Decompress(args) => decompress::run(args).await,
			Command::Download(args) => download::run(args).await,
			Command::Extract(args) => extract::run(args).await,
		};

		if let Err(error) = &result
			&& let (Some(tangram_output_path), Some(output_path)) = (
				std::env::var_os("TANGRAM_OUTPUT").map(PathBuf::from),
				output_path.as_deref(),
			) && tangram_output_path == output_path
		{
			// Get the error data.
			let Some(data) = error
				.state()
				.object()
				.map(|object| object.unwrap_error_ref().to_data())
			else {
				return result;
			};

			// Create the output if the builtin did not, because the xattr requires it.
			if tokio::fs::symlink_metadata(&tangram_output_path)
				.await
				.is_err()
			{
				tokio::fs::write(&tangram_output_path, b"").await.map_err(
					|error| tg::error!(!error, path = %tangram_output_path.display(), "failed to write the output"),
				)?;
			}

			// Write the error xattr.
			let json = serde_json::to_vec(&data)
				.map_err(|error| tg::error!(!error, "failed to serialize the error"))?;
			xattr::set(&tangram_output_path, "user.tangram.error", &json)
				.map_err(|error| tg::error!(!error, "failed to write the error xattr"))?;
		}

		result
	}
}
