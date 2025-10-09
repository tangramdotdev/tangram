use {
	crate::Cli,
	tangram_client::{self as tg, prelude::*},
};

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(long, short)]
	pub force: bool,

	#[arg(default_value = ".", index = 2)]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(long, require_equals = true, short)]
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
		let arg = tg::get::Arg {
			checkin: args.checkin.to_options(),
		};
		let referent = self.get_reference_with_arg(&args.reference, arg).await?;
		let item = referent
			.item
			.map_left(|process| process.id().clone())
			.map_right(|object| object.id().clone());

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
