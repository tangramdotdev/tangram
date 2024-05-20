use crate::Cli;
use tangram_client as tg;

/// Archive an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: tg::artifact::archive::Format,

	pub artifact: tg::artifact::Id,
}

impl Cli {
	pub async fn command_artifact_archive(&self, args: Args) -> tg::Result<()> {
		let artifact = tg::Artifact::with_id(args.artifact);
		let format = args.format;
		let target = artifact.archive_target(format);
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
