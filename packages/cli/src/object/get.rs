use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the object's metadata.
	#[arg(long)]
	pub metadata: bool,

	/// The object to print.
	#[arg(index = 1)]
	pub object: tg::Referent<tg::object::Id>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_get(&mut self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let id = args.object.item.clone();
		let token = args.object.options.token.clone();
		if args.bytes {
			let arg = tg::object::get::Arg {
				location: args.locations.get(),
				metadata: args.metadata,
				token: token.clone(),
			};
			let tg::object::get::Output { bytes, metadata } = client
				.try_get_object(&id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
			if let Some(metadata) = metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
				self.print_info_message(&metadata);
			}
			tokio::io::stdout()
				.write_all(&bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			return Ok(());
		}
		let object = tg::Object::with_referent(args.object);
		let value = tg::Value::Object(object);
		args.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		let arg = tg::object::get::Arg {
			location: args.locations.get(),
			metadata: args.metadata,
			token,
		};
		if args.metadata {
			let output = client
				.try_get_object(&id, arg.clone())
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
			if let Some(metadata) = output.metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
				self.print_info_message(&metadata);
			}
		}
		self.print_value(&value, args.print, arg).await?;
		Ok(())
	}
}
