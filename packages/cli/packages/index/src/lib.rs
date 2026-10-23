use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Index processes and objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_index(&mut self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let stream = client
			.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;
		self.render_progress_stream(stream).await?;
		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
