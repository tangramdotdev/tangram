use crate::Cli;
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long)]
	pub force: bool,

	#[arg(index = 2, default_value = ".")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	#[arg(index = 1)]
	pub tag: Option<tg::Tag>,
}

impl Cli {
	pub async fn command_tag_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(process) => Either::Left(process.id().clone()),
			Either::Right(object) => Either::Right(object.id().clone()),
		};

		// Put the tag.
		let arg = tg::tag::put::Arg {
			force: args.force,
			item,
			remote,
		};
		handle.put_tag(&args.tag.unwrap(), arg).await?;

		Ok(())
	}
}
