use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Put a tag.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub ancestors: tangram_cli_core::node::Ancestors,

	#[usage(flatten)]
	pub checkin: tangram_cli_checkin::Options,

	/// Replace a conflicting named node and its descendants.
	#[usage(long, short)]
	pub force: bool,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(long = "public")]
	pub public: bool,

	pub specifier: Option<tg::Specifier>,

	#[usage(default = ".")]
	pub reference: tg::Reference,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;

	// Get the reference.
	let mut reference = args.reference.clone();
	let mut options = reference.options().clone();
	options.follow = true;
	reference.set_options(options);
	let arg = tg::get::Arg {
		checkin: args.checkin.to_options(),
		..Default::default()
	};
	let referent = tangram_cli_reference::get_with_arg(cli, &reference, arg)
		.await?
		.referent;
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
		tokens: referent.options.tokens,
		ancestors: args.ancestors.get(),
		force: args.force,
		location: args.location.get(),
		public: args.public,
		specifier: args
			.specifier
			.clone()
			.ok_or_else(|| tg::error!("expected a specifier"))?,
		target,
	};
	client
		.put_tag(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to put the tag"))?;

	Ok(())
}
