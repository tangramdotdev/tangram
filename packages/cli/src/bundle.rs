use {crate::Cli, tangram_client as tg};

/// Bundle an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub artifact: tg::artifact::Id,

	#[command(flatten)]
	pub build: crate::build::Options,
}

impl Cli {
	pub async fn command_bundle(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let artifact = tg::Artifact::with_id(args.artifact);
		let command = tg::builtin::bundle_command(&artifact);
		let command = command.store(&handle).await?;
		let reference = tg::Reference::with_object(command.into());
		let args = crate::build::Args {
			options: args.build,
			reference: Some(reference),
			trailing: Vec::new(),
		};
		self.command_build(args).await?;
		Ok(())
	}
}
