use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*};

mod archive;
mod checksum;
mod compress;
mod decompress;
mod download;
mod extract;
mod progress;
mod util;

pub use tangram_cli_builtin_config as config;
#[cfg(any(feature = "quickjs", feature = "v8"))]
pub use tangram_cli_builtin_javascript as javascript;
#[cfg(feature = "cpython")]
pub use tangram_cli_builtin_python as python;

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
	Config(config::Args),
	Decompress(decompress::Args),
	Download(download::Args),
	Extract(extract::Args),
	#[cfg(any(feature = "quickjs", feature = "v8"))]
	#[command(name = "javascript", alias = "js")]
	JavaScript(javascript::Args),
	#[cfg(feature = "cpython")]
	#[command(alias = "py")]
	Python(python::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let (named, positional) = match &args.command {
		Command::Archive(args) => (&args.output_named, &args.output_positional),
		Command::Checksum(args) => (&args.output_named, &args.output_positional),
		Command::Compress(args) => (&args.output_named, &args.output_positional),
		Command::Config(_) => (&None, &None),
		Command::Decompress(args) => (&args.output_named, &args.output_positional),
		Command::Download(args) => (&args.output_named, &args.output_positional),
		Command::Extract(args) => (&args.output_named, &args.output_positional),
		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::JavaScript(_) => (&None, &None),
		#[cfg(feature = "cpython")]
		Command::Python(_) => (&None, &None),
	};
	let output_path = util::resolve_path(named.clone(), positional.clone());

	let progress_interval = cli.builtin_config().progress_interval;
	let result = match args.command {
		Command::Archive(args) => archive::run(args, progress_interval).await,
		Command::Checksum(args) => checksum::run(args, progress_interval).await,
		Command::Compress(args) => compress::run(args, progress_interval).await,
		Command::Config(args) => config::run(cli, args).await,
		Command::Decompress(args) => decompress::run(args, progress_interval).await,
		Command::Download(args) => download::run(args, progress_interval).await,
		Command::Extract(args) => extract::run(args, progress_interval).await,
		#[cfg(any(feature = "quickjs", feature = "v8"))]
		Command::JavaScript(args) => javascript::run(cli, args).await,
		#[cfg(feature = "cpython")]
		Command::Python(args) => python::run(cli, args).await,
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
		tg::file::xattrs::write_error(&tangram_output_path, &json)
			.map_err(|error| tg::error!(!error, "failed to write the error xattr"))?;
	}

	result
}
