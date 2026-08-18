use {crate::Cli, tangram_client::prelude::*};

/// Put a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub ancestors: crate::node::Ancestors,

	#[command(flatten)]
	pub checkin: crate::checkin::Options,

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
		let mut reference = args.reference.clone();
		let mut options = reference.options().clone();
		options.follow = true;
		reference.set_options(options);
		let arg = tg::get::Arg {
			checkin: args.checkin.to_options(),
			..Default::default()
		};
		let referent = self.get_with_arg(&reference, arg).await?.referent;
		let target = match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => id
				.try_into()
				.map(|id: tg::process::Id| tg::tag::data::Target::from(id))?,
			tg::get::Node::Id(id) => id
				.try_into()
				.map(|id: tg::object::Id| tg::tag::data::Target::from(id))?,
			tg::get::Node::Pointer(_) => return Err(tg::error!("expected an ID")),
		};

		// Put the tag.
		let arg = tg::tag::put::Arg {
			ancestors: args.ancestors.get(),
			target,
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
