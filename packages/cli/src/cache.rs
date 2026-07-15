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
		let client = self.client().await?;

		// Resolve the references to artifact IDs.
		let referents = self.get_references(&args.references).await?;
		let artifacts = referents
			.into_iter()
			.map(|referent| {
				let edge = referent.into_graph_edge()?.item;
				let object = edge
					.try_unwrap_object()
					.map_err(|_| tg::error!("expected an object"))?;
				let artifact = tg::Artifact::try_from(object)?;
				let artifact = artifact.state().token().map_or_else(
					|| tg::Either::Left(artifact.id()),
					|token| {
						tg::Either::Right(tg::WithToken {
							id: artifact.id(),
							token,
						})
					},
				);
				Ok(artifact)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let arg = tg::cache::Arg { artifacts };
		let stream = client
			.cache(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to create the cache stream"))?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
