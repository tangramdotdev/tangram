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
	pub inner: crate::target::build::InnerArgs,
}

impl Cli {
	pub async fn command_artifact_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let algorithm = args.algorithm;
		let target = artifact.checksum_target(algorithm);
		let target = target.id(&handle).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(&target.into())),
			inner: args.inner,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
