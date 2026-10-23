use {tangram_cli_core::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Run the language server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_lsp(&mut self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let stdin = Box::new(StreamReader::new(
			tangram_util::io::stdin()
				.map_err(|error| tg::error!(!error, "failed to open stdin"))?,
		));
		let stdout = Box::new(tokio::io::BufWriter::new(tokio::io::stdout()));
		client
			.lsp(stdin, stdout)
			.await
			.map_err(|error| tg::error!(!error, "failed to run the language server"))?;
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
