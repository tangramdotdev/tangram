use {crate::Cli, tangram_client::prelude::*};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	/// The object to print.
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// The remote to get the object from.
	#[arg(long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_get(&mut self, mut args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let value = if args.bytes {
			let arg = tg::object::get::Arg {
				remote: args.remote.clone(),
			};
			let tg::object::get::Output { bytes } = handle
				.try_get_object(&args.object, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to get the object"))?;
			tg::Value::Bytes(bytes)
		} else {
			tg::Value::Object(tg::Object::with_id(args.object))
		};
		args.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		self.print(&value, args.print).await?;
		Ok(())
	}
}
