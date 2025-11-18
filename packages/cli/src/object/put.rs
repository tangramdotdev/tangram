use {
	crate::{Cli, put::Format},
	tangram_client::prelude::*,
	tokio::io::AsyncReadExt as _,
};

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

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes()
		} else {
			let mut bytes = Vec::new();
			crate::util::stdio::stdin()
				.read_to_end(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};

		let bytes = match args.format {
			Format::Bytes => bytes.into(),
			Format::Json => {
				let data: tg::object::Data = serde_json::from_slice(&bytes)
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

		let value = tg::Value::Object(tg::Object::with_id(id));
		self.print(&value, args.print).await?;

		Ok(())
	}
}
