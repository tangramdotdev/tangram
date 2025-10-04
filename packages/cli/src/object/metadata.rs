use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Get object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(long)]
	pub pretty: Option<bool>,

	/// The remote to get the metadata from.
	#[arg(long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_metadata(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::object::metadata::Arg {
			remote: args.remote,
		};
		let metadata = handle.try_get_object_metadata(&args.object, arg).await.map_err(
			|source| tg::error!(!source, %id = args.object, "failed to get the object metadata"),
		)?.ok_or_else(|| tg::error!("failed to get the object metadata"))?;
		Self::print_json(&metadata, args.pretty).await?;
		Ok(())
	}
}
