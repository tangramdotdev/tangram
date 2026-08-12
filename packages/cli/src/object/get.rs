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
	pub object: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Get the object's storage status.
	#[arg(long)]
	pub stored: bool,
}

impl Cli {
	pub async fn command_object_get(&mut self, mut args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let object = self.resolve_object(&args.object).await?;
		let id = object.node.clone();
		let tokens = object.options.tokens.clone();
		if args.bytes {
			let arg = tg::object::get::Arg {
				location: args.locations.get(),
				metadata: args.metadata,
				stored: args.stored,
				tokens: tokens.clone(),
			};
			let tg::object::get::Output {
				bytes,
				metadata,
				stored,
				tokens: _,
			} = client
				.try_get_object(&id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
			if let Some(metadata) = metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
				self.print_info_message(&metadata);
			}
			if let Some(stored) = stored {
				let stored = serde_json::to_string(&stored).map_err(|error| {
					tg::error!(!error, "failed to serialize the storage status")
				})?;
				self.print_info_message(&stored);
			}
			tokio::io::stdout()
				.write_all(&bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			return Ok(());
		}
		let object = tg::Object::with_referent(object);
		let value = tg::Value::Object(object);
		args.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		let arg = tg::object::get::Arg {
			location: args.locations.get(),
			metadata: args.metadata,
			stored: args.stored,
			tokens,
		};
		if args.metadata || args.stored {
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
			if let Some(stored) = output.stored {
				let stored = serde_json::to_string(&stored).map_err(|error| {
					tg::error!(!error, "failed to serialize the storage status")
				})?;
				self.print_info_message(&stored);
			}
		}
		self.print_value(&value, args.print, arg).await?;
		Ok(())
	}
}
