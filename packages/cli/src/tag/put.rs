use {crate::Cli, tangram_client::prelude::*};

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(long, short)]
	pub force: bool,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(default_value = ".", index = 2)]
	pub reference: tg::Reference,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,

	#[arg(index = 1)]
	pub tag: Option<tg::Tag>,
}

impl Cli {
	pub async fn command_tag_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let arg = tg::get::Arg {
			checkin: args.checkin.to_options(),
			..Default::default()
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
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		handle.put_tag(&args.tag.unwrap(), arg).await?;

		Ok(())
	}
}
