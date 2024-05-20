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
		let artifact = tg::Artifact::with_id(args.artifact);
		let target = artifact.bundle_target();
		let target = target.id(&self.handle, None).await?;
		let args = crate::target::build::InnerArgs {
			target: Some(target),
			..Default::default()
		};
		let output = self.command_target_build_inner(args, false).await?;
		let output = output.unwrap().unwrap_value();
		println!("{output}");
		Ok(())
	}
}
