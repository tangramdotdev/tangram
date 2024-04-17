use crate::Cli;
use tangram_client as tg;

/// Run the language server.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&self, _args: Args) -> tg::Result<()> {
		let client = &self.client().await?;

		let stdin = Box::new(tokio::io::BufReader::new(tokio::io::stdin()));
		let stdout = Box::new(tokio::io::stdout());

		client.lsp(stdin, stdout).await?;

		Ok(())
	}
}
