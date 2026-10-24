use {tangram_cli_core::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader};

/// Write a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,

	#[command(flatten)]
	pub checkout_pointers: tangram_cli_checkin::CheckoutPointers,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn command_write(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::write::Arg {
		checkout_pointers: args.checkout_pointers.get_option(),
	};
	let output = if let Some(bytes) = args.bytes {
		let reader = std::io::Cursor::new(bytes);
		client
			.write(arg, reader)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the blob"))?
	} else {
		let reader = StreamReader::new(
			tangram_util::io::stdin()
				.map_err(|error| tg::error!(!error, "failed to open stdin"))?,
		);
		client
			.write(arg, reader)
			.await
			.map_err(|error| tg::error!(!error, "failed to write the blob"))?
	};
	Cli::print_referent(&output.blob, &args.print);
	Ok(())
}
