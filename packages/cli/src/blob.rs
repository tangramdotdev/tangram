use crate::Cli;
use tangram_client as tg;

pub mod cat;
pub mod checksum;
pub mod compress;
pub mod decompress;
pub mod download;

/// Manage blobs.
#[derive(Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Cat(self::cat::Args),
	Checksum(self::checksum::Args),
	Compress(self::compress::Args),
	Decompress(self::decompress::Args),
	Download(self::download::Args),
}

impl Cli {
	pub async fn command_blob(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Cat(args) => {
				self.command_blob_cat(args).await?;
			},
			Command::Checksum(args) => {
				self.command_blob_checksum(args).await?;
			},
			Command::Compress(args) => {
				self.command_blob_compress(args).await?;
			},
			Command::Decompress(args) => {
				self.command_blob_decompress(args).await?;
			},
			Command::Download(args) => {
				self.command_blob_download(args).await?;
			},
		}
		Ok(())
	}
}
