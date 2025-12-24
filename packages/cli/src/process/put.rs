use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncReadExt as _};

// Put a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub bytes: Option<String>,

	#[arg(index = 1)]
	pub id: tg::process::Id,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_process_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let bytes = if let Some(bytes) = args.bytes {
			bytes
		} else {
			let mut bytes = String::new();
			crate::util::stdio::stdin()
				.read_to_string(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};
		let data = serde_json::from_str(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deseralize the data"))?;
		let arg = tg::process::put::Arg {
			data,
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		handle
			.put_process(&args.id, arg)
			.await
			.map_err(|source| tg::error!(!source, id = %args.id, "failed to put the process"))?;
		Ok(())
	}
}
