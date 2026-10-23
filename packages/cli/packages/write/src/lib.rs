use {tangram_cli_core::Cli, tangram_client::prelude::*, tokio_util::io::StreamReader};

/// The component.
pub struct Component<'a>(&'a mut Cli);

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

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_write(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
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
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
