use crate::Cli;
use tangram_client as tg;

pub mod archive;
pub mod bundle;
pub mod cat;
pub mod checkin;
pub mod checkout;
pub mod checksum;
pub mod extract;

/// Manage artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Archive(self::archive::Args),
	Bundle(self::bundle::Args),
	Cat(self::cat::Args),
	Checkin(self::checkin::Args),
	Checkout(self::checkout::Args),
	Checksum(self::checksum::Args),
	Extract(self::extract::Args),
}

impl Cli {
	pub async fn command_artifact(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Archive(args) => {
				self.command_artifact_archive(args).await?;
			},
			Command::Bundle(args) => {
				self.command_artifact_bundle(args).await?;
			},
			Command::Cat(args) => {
				self.command_artifact_cat(args).await?;
			},
			Command::Checkin(args) => {
				self.command_artifact_checkin(args).await?;
			},
			Command::Checkout(args) => {
				self.command_artifact_checkout(args).await?;
			},
			Command::Checksum(args) => {
				self.command_artifact_checksum(args).await?;
			},
			Command::Extract(args) => {
				self.command_artifact_extract(args).await?;
			},
		}
		Ok(())
	}
}
