use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;

/// Extract an artifact from a blob or file.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub item: Either<tg::blob::Id, tg::file::Id>,

	#[command(flatten)]
	pub build: crate::process::build::Options,
}

impl Cli {
	pub async fn command_artifact_extract(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = match args.item {
			Either::Left(id) => tg::Blob::with_id(id),
			Either::Right(id) => {
				let file = tg::File::with_id(id);
				file.contents(&handle).await?
			},
		};
		let command = tg::Artifact::extract_command(&blob);
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build_process(args.build, reference, vec![]).await?;
		Ok(())
	}
}
