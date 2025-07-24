use crate::Cli;
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncReadExt as _;

/// Put an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub bytes: Option<String>,

	#[arg(index = 1)]
	pub id: Option<tg::object::Id>,

	#[arg(short, long)]
	pub kind: Option<tg::object::Kind>,
}

impl Cli {
	pub async fn command_object_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes().into()
		} else {
			let mut bytes = Vec::new();
			tokio::io::BufReader::new(tokio::io::stdin())
				.read_to_end(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes.into()
		};
		let id = if let Some(id) = args.id {
			id
		} else {
			let kind = args.kind.ok_or_else(|| tg::error!("kind must be set"))?;
			tg::object::Id::new(kind, &bytes)
		};
		let arg = tg::object::put::Arg { bytes };
		handle.put_object(&id, arg).await?;
		println!("{id}");
		Ok(())
	}
}
