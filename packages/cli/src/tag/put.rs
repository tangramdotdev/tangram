use {crate::Cli, tangram_client::prelude::*};

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub checkin: crate::checkin::Options,

	#[arg(id = "put.force", long = "force", short = 'f')]
	pub force: bool,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(id = "put.public", long = "public")]
	pub public: bool,

	#[arg(default_value = ".", index = 2)]
	pub reference: tg::Reference,

	#[arg(index = 1)]
	pub specifier: Option<tg::Specifier>,
}

impl Cli {
	pub async fn command_tag_put(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Get the reference.
		let arg = tg::get::Arg {
			checkin: args.checkin.to_options(),
			resolve: true,
			..Default::default()
		};
		let referent = self.get_reference_with_arg(&args.reference, arg).await?;
		let item = match referent.item {
			tg::get::Item::Id(id) if id.kind() == tg::id::Kind::Process => id
				.try_into()
				.map(|id: tg::process::Id| tg::tag::data::Item::from(id))?,
			tg::get::Item::Id(id) => id
				.try_into()
				.map(|id: tg::object::Id| tg::tag::data::Item::from(id))?,
			tg::get::Item::Pointer(_) => return Err(tg::error!("expected an ID")),
		};

		// Put the tag.
		let arg = tg::tag::put::Arg {
			force: args.force,
			item,
			location: args.location.get(),
			public: args.public,
			specifier: args
				.specifier
				.clone()
				.ok_or_else(|| tg::error!("expected a specifier"))?,
		};
		client
			.put_tag(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to put the tag"))?;

		Ok(())
	}
}
