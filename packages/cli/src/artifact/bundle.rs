use crate::Cli;
use tangram_client as tg;

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub artifact: tg::artifact::Id,
}

impl Cli {
	pub async fn command_artifact_bundle(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let target = artifact.bundle_target();
		let target = target.id(&client).await?;
		let args = crate::target::build::Args {
			inner: crate::target::build::InnerArgs {
				target: Some(target),
				..Default::default()
			},
			detach: false,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
