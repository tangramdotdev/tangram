use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_extract(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = self.resolve_object(&args.reference).await?;
		let blob = tg::Object::with_referent(blob)
			.try_unwrap_blob()
			.map_err(|_| tg::error!("expected a blob"))?;
		let command = tg::builtin::extract_command(&blob);
		let command = command
			.store_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
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
