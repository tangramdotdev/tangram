use crate::Cli;

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

impl Cli {
	pub async fn command_builtin(&mut self, args: Args) -> tangram_client::Result<()> {
		match args.command {
			Command::Archive(args) => archive::run(args).await,
			Command::Checksum(args) => checksum::run(args).await,
			Command::Compress(args) => compress::run(args).await,
			Command::Decompress(args) => decompress::run(args).await,
			Command::Download(args) => download::run(args).await,
			Command::Extract(args) => extract::run(args).await,
		}
	}
}
