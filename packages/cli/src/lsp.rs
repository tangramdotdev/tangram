use {crate::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader};

/// Run the language server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&mut self, _args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let stdin = Box::new(StreamReader::new(
			tangram_util::io::stdin()
				.map_err(|source| tg::error!(!source, "failed to open stdin"))?,
		));
		let stdout = Box::new(tokio::io::BufWriter::new(tokio::io::stdout()));
		client
			.lsp(stdin, stdout)
			.await
			.map_err(|source| tg::error!(!source, "failed to run the language server"))?;
		Ok(())
	}
}
