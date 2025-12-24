use {crate::Cli, tangram_client::prelude::*};

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[command(flatten)]
	pub build: crate::build::Options,
}

impl Cli {
	pub async fn command_extract(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let blob = tg::Blob::with_id(args.blob);
		let command = tg::builtin::extract_command(&blob);
		let command = command
			.store(&handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let args = crate::build::Args {
			options: args.build,
			reference,
			trailing: Vec::new(),
		};
		self.command_build(args).await?;
		Ok(())
	}
}
