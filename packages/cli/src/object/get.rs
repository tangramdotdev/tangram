use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	/// Get the object's metadata.
	#[arg(long)]
	pub metadata: bool,

	/// The object to print.
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_object_get(&mut self, mut args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		if args.bytes {
			let arg = tg::object::get::Arg {
				local: args.local.local,
				metadata: args.metadata,
				remotes: args.remotes.remotes.clone(),
			};
			let tg::object::get::Output { bytes, metadata } = handle
				.try_get_object(&args.object, arg)
				.await
				.map_err(
					|source| tg::error!(!source, id = %args.object, "failed to get the object"),
				)?
				.ok_or_else(|| tg::error!(id = %args.object, "failed to find the object"))?;
			if let Some(metadata) = metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
				Self::print_info_message(&metadata);
			}
			tokio::io::stdout()
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			return Ok(());
		}
		let value = tg::Value::Object(tg::Object::with_id(args.object.clone()));
		args.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		let arg = tg::object::get::Arg {
			local: args.local.local,
			metadata: args.metadata,
			remotes: args.remotes.remotes.clone(),
		};
		if args.metadata {
			let output = handle
				.try_get_object(&args.object, arg.clone())
				.await
				.map_err(
					|source| tg::error!(!source, id = %args.object, "failed to get the object"),
				)?
				.ok_or_else(|| tg::error!(id = %args.object, "failed to find the object"))?;
			if let Some(metadata) = output.metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize the metadata"))?;
				Self::print_info_message(&metadata);
			}
		}
		self.print_value(&value, args.print, arg).await?;
		Ok(())
	}
}
