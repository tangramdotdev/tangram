use crate::{Cli, put::Format};
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncReadExt as _;

/// Put an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub bytes: Option<String>,

	#[arg(long, default_value = "bytes")]
	pub format: Format,

	#[arg(index = 1)]
	pub id: Option<tg::object::Id>,

	#[arg(long, short)]
	pub kind: Option<tg::object::Kind>,
}

impl Cli {
	pub async fn command_object_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let input = if let Some(bytes) = args.bytes {
			bytes
		} else {
			let mut input = String::new();
			crate::util::stdio::stdin()
				.read_to_string(&mut input)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			input
		};

		let bytes = match args.format {
			Format::Bytes => input.into_bytes().into(),
			Format::Json => {
				let data: tg::object::Data = serde_json::from_str(&input)
					.map_err(|source| tg::error!(!source, "failed to parse JSON"))?;
				data.serialize()?
			},
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
