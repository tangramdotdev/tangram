use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncWriteExt};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub local: crate::util::args::Local,

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
				remotes: args.remotes.remotes.clone(),
			};
			let tg::object::get::Output { bytes } = handle
				.try_get_object(&args.object, arg)
				.await?
				.ok_or_else(|| tg::error!(id = %args.object, "failed to get the object"))?;
			tokio::io::stdout()
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			return Ok(());
		}
		let value = tg::Value::Object(tg::Object::with_id(args.object));
		args.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		let arg = tg::object::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes.clone(),
		};
		self.print_value(&value, args.print, arg).await?;
		Ok(())
	}
}
