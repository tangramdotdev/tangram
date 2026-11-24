use {
	tangram_cli_core::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader,
	usage_rs as usage,
};

/// Run the language server.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {}

pub async fn run(cli: &mut Cli, _args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let stdin = Box::new(StreamReader::new(
		tangram_util::io::stdin().map_err(|error| tg::error!(!error, "failed to open stdin"))?,
	));
	let stdout = Box::new(tokio::io::BufWriter::new(tokio::io::stdout()));
	client
		.lsp(stdin, stdout)
		.await
		.map_err(|error| tg::error!(!error, "failed to run the language server"))?;
	Ok(())
}
