use crate::Cli;
use tangram_error::Result;

/// Run the language server.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&self, _args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Create the language server.
		let server = tangram_server::language::Server::new(client, tokio::runtime::Handle::current());

		// Run the language server.
		server.serve().await?;

		Ok(())
	}
}
