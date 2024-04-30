use crate::Cli;
use tangram_client as tg;
use tg::Handle as _;

/// Run the language server.
#[derive(Debug, clap::Args)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&self, _args: Args) -> tg::Result<()> {
		let stdin = Box::new(tokio::io::BufReader::new(tokio::io::stdin()));
		let stdout = Box::new(tokio::io::stdout());

		self.handle.lsp(stdin, stdout).await?;

		Ok(())
	}
}