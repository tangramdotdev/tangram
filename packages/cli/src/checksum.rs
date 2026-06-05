use {crate::Cli, futures::FutureExt as _, tangram_client::prelude::*};

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(default_value = "sha256", long)]
	pub algorithm: tg::checksum::Algorithm,

	#[command(flatten)]
	pub build: crate::process::build::Options,

	/// The artifact, blob, or URL to checksum.
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_checksum(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let referent = self.get_resolved_reference(&args.reference).await?;
		let edge = crate::get::get_item_to_graph_edge(referent.item)?;
		let object = edge
			.try_unwrap_object()
			.map_err(|_| tg::error!("expected an object"))?;
		if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(tg::Either::Left(blob), algorithm);
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
		} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			let algorithm = args.algorithm;
			let command = tg::builtin::checksum_command(tg::Either::Right(artifact), algorithm);
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
		} else {
			return Err(tg::error!("expected an artifact or a blob"));
		}
		Ok(())
	}
}
