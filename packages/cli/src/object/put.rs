use crate::Cli;
use tangram_client as tg;

use tokio::io::AsyncReadExt as _;

/// Put an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	bytes: Option<String>,

	#[arg(short, long)]
	kind: tg::object::Kind,
}

impl Cli {
	pub async fn command_object_put(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let kind = args.kind.into();
		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes()
		} else {
			let mut bytes = Vec::new();
			tokio::io::stdin()
				.read_to_end(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};
		let id = tg::Id::new_blake3(kind, &bytes).try_into().unwrap();
		let arg = tg::object::put::Arg {
			bytes: bytes.into(),
		};
		client.put_object(&id, arg).await?;
		println!("{id}");
		Ok(())
	}
}
