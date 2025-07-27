use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Run the language server.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {}

impl Cli {
	pub async fn command_lsp(&mut self, _args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let stdin = Box::new(crate::util::stdio::stdin());
		let stdout = Box::new(tokio::io::BufWriter::new(tokio::io::stdout()));
		handle.lsp(stdin, stdout).await?;
		Ok(())
	}
}
