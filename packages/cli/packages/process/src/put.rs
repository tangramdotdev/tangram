use {
	tangram_cli_core::Cli, tangram_client::prelude::*, tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader, usage_rs as usage,
};

// Put a process.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub id: tg::process::Id,

	pub bytes: Option<String>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	/// Grant public read access.
	#[usage(long)]
	pub public: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let bytes = if let Some(bytes) = args.bytes {
		bytes
	} else {
		let mut bytes = String::new();
		StreamReader::new(
			tangram_util::io::stdin()
				.map_err(|error| tg::error!(!error, "failed to open stdin"))?,
		)
		.read_to_string(&mut bytes)
		.await
		.map_err(|error| tg::error!(!error, "failed to read stdin"))?;
		bytes
	};
	let data = serde_json::from_str(&bytes)
		.map_err(|error| tg::error!(!error, "failed to deserialize the data"))?;
	let arg = tg::process::put::Arg {
		data,
		location: args.location.get(),
		public: args.public,
	};
	client
		.put_process(&args.id, arg)
		.await
		.map_err(|error| tg::error!(!error, id = %args.id, "failed to put the process"))?;
	Ok(())
}
