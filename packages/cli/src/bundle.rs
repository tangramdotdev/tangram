use crate::Cli;
use tangram_client as tg;

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub build: crate::process::build::Options,
}

impl Cli {
	pub async fn command_bundle(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let command = tg::builtin::bundle_command(&artifact);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build_process(args.build, reference, vec![]).await?;
		Ok(())
	}
}
