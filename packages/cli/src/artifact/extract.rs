use crate::Cli;
use tangram_client as tg;

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub value: tg::object::Id,

	#[command(flatten)]
	pub build: crate::process::build::Options,
}

impl Cli {
	pub async fn command_artifact_extract(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = match args.value {
			tg::object::Id::Leaf(id) => tg::Blob::with_id(id.into()),
			tg::object::Id::Branch(id) => tg::Blob::with_id(id.into()),
			tg::object::Id::File(id) => {
				let file = tg::File::with_id(id.into());
				file.contents(&handle).await?
			},
			_ => return Err(tg::error!("expected a blob or a file")),
		};
		let command = tg::Artifact::extract_command(&blob);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build_process(args.build, reference, vec![]).await?;
		Ok(())
	}
}
