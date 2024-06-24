use crate::Cli;
use tangram_client::{self as tg, Handle as _};

/// Run the language server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let stdin = Box::new(tokio::io::BufReader::new(tokio::io::stdin()));
		let stdout = Box::new(tokio::io::stdout());

		handle.lsp(stdin, stdout).await?;

		Ok(())
	}
}
