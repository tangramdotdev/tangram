use crate::Cli;
use tangram_error::Result;

/// Run the language server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&self, _args: Args) -> Result<()> {
		let client = &self.client().await?;

		let stdin = Box::new(tokio::io::stdin());
		let stdout = Box::new(tokio::io::stdout());

		client.lsp(stdin, stdout).await?;

		Ok(())
	}
}
