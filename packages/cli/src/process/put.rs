use {
	crate::Cli, tangram_client::prelude::*, tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader,
};

// Put a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub bytes: Option<String>,

	#[arg(index = 1)]
	pub id: tg::process::Id,

	#[command(flatten)]
	pub location: crate::location::Location,
}

impl Cli {
	pub async fn command_process_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let bytes = if let Some(bytes) = args.bytes {
			bytes
		} else {
			let mut bytes = String::new();
			StreamReader::new(
				tangram_util::io::stdin()
					.map_err(|source| tg::error!(!source, "failed to open stdin"))?,
			)
			.read_to_string(&mut bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};
		let data = serde_json::from_str(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deseralize the data"))?;
		let arg = tg::process::put::Arg {
			data,
			location: args.location.get()?,
		};
		handle
			.put_process(&args.id, arg)
			.await
			.map_err(|source| tg::error!(!source, id = %args.id, "failed to put the process"))?;
		Ok(())
	}
}
