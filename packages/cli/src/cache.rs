use {crate::Cli, tangram_client::prelude::*};

/// Cache an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(required = true)]
	pub references: Vec<tg::Reference>,
}

impl Cli {
	pub async fn command_cache(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Resolve the references to artifact IDs.
		let referents = self.get_references(&args.references).await?;
		let artifacts = referents
			.into_iter()
			.map(|referent| {
				let tg::Either::Left(object) = referent.item else {
					return Err(tg::error!("expected an object"));
				};
				let artifact = tg::Artifact::try_from(object)?;
				Ok(artifact.id())
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let arg = tg::cache::Arg { artifacts };
		let stream = handle
			.cache(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the cache stream"))?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
