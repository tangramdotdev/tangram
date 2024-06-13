use crate::Cli;
use tangram_client as tg;

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub blob: tg::blob::Id,

	#[arg(long)]
	pub format: Option<tg::artifact::archive::Format>,
}

impl Cli {
	pub async fn command_artifact_extract(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let target = tg::Artifact::extract_target(&blob, format);
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
