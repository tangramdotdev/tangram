use crate::Cli;
use tangram_client as tg;

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<tg::artifact::archive::Format>,

	pub blob: tg::blob::Id,
}

impl Cli {
	pub async fn command_artifact_extract(&self, args: Args) -> tg::Result<()> {
		let blob = tg::Blob::with_id(args.blob);
		let format = args.format;
		let target = tg::Artifact::extract_target(&blob, format);
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
