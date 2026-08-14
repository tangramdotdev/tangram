use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The object to print.
	#[arg(index = 1)]
	pub object: tg::Reference,

	#[command(flatten)]
	pub options: Options,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the object's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Get the object's storage status.
	#[arg(long)]
	pub stored: bool,
}

impl Cli {
	pub async fn command_object_get(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_locations(&args.object, &args.options.locations)
			.await?;
		self.command_object_get_inner(object, args.options).await
	}

	pub(crate) async fn command_object_get_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		mut options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = object.node.clone();
		let location = object.options.location.clone().map(Into::into);
		let tokens = object.options.tokens.clone();
		if options.bytes {
			let arg = tg::object::get::Arg {
				location: location.clone(),
				metadata: options.metadata,
				stored: options.stored,
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
		options
			.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		if options.metadata {
			let metadata = object
				.metadata_with_handle(&client)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object metadata"))?;
			let metadata = serde_json::to_string(&metadata)
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
			self.print_info_message(&metadata);
		}
		if options.stored {
			let stored = object.stored_with_handle(&client).await.map_err(
				|error| tg::error!(!error, %id, "failed to get the object's storage status"),
			)?;
			let stored = serde_json::to_string(&stored)
				.map_err(|error| tg::error!(!error, "failed to serialize the storage status"))?;
			self.print_info_message(&stored);
		}
		let value = tg::Value::Object(object);
		self.print_value(&value, options.print, None).await?;
		Ok(())
	}
}
