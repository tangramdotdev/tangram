use crate::Cli;
use tangram_client as tg;

/// Checkum an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,

	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub inner: crate::process::run::InnerArgs,
}

impl Cli {
	pub async fn command_artifact_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let algorithm = args.algorithm;
		let command = artifact.checksum_command(algorithm);
		let command = command.id(&handle).await?;
		let args = crate::process::build::Args {
			reference: Some(tg::Reference::with_object(&command.into())),
			inner: args.inner,
		};
		self.command_process_build(args).await?;
		Ok(())
	}
}
