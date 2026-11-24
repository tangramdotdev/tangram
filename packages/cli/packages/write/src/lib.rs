use {
	tangram_cli_core::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader,
	usage_rs as usage,
};

/// Write a blob.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub bytes: Option<String>,

	#[usage(flatten)]
	pub checkout_pointers: tangram_cli_checkin::CheckoutPointers,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
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
