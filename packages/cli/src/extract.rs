use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub blob: tg::blob::Id,

	#[command(flatten)]
	pub build: crate::process::build::Options,
}

impl Cli {
	pub async fn command_extract(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = tg::Blob::with_id(args.blob);
		let command = tg::builtin::extract_command(&blob);
		let command = command
			.store_with_handle(&client)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let args = crate::process::build::Args {
			options: args.build,
			reference: Some(reference),
			trailing: Vec::new(),
		};
		self.command_build(args).boxed().await?;
		Ok(())
	}
}
