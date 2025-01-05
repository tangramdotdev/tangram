use crate::Cli;
use tangram_client as tg;

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub inner: crate::target::build::InnerArgs,
}

impl Cli {
	pub async fn command_artifact_bundle(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let target = artifact.bundle_target();
		let target = target.id(&handle).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(&target.into())),
			inner: args.inner,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
