use {crate::Cli, tangram_client::prelude::*};

/// Cache an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(required = true)]
	pub artifacts: Vec<tg::artifact::Id>,
}

impl Cli {
	pub async fn command_cache(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::cache::Arg {
			artifacts: args.artifacts,
		};
		let stream = handle
			.cache(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the cache stream"))?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}
