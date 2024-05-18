use crate::Cli;
use tangram_client as tg;

/// Checkum an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub algorithm: tg::checksum::Algorithm,

	pub artifact: tg::artifact::Id,
}

impl Cli {
	pub async fn command_artifact_checksum(&self, args: Args) -> tg::Result<()> {
		let artifact = tg::Artifact::with_id(args.artifact);
		let algorithm = args.algorithm;
		let target = artifact.checksum_target(algorithm);
		let target = target.id(&self.handle, None).await?;
		let args = crate::target::build::InnerArgs {
			target: Some(target.to_string()),
			..Default::default()
		};
		let output = self.command_target_build_inner(args, false).await?;
		let output = output.unwrap().unwrap_value();
		println!("{output}");
		Ok(())
	}
}
