use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Push nodes.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub ancestors: tangram_cli_core::node::Options,

	#[usage(flatten)]
	pub destination: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub eager: Eager,

	/// Replace conflicting named nodes and their descendants.
	#[usage(long, short)]
	pub force: bool,

	#[usage(long)]
	pub group_children: bool,

	#[usage(long)]
	pub metadata: bool,

	#[usage(long)]
	pub organization_children: bool,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[usage(long)]
	pub process_children: bool,

	#[usage(long)]
	pub process_command_objects: bool,

	#[usage(flatten)]
	pub process_error_objects: ProcessErrorObjects,

	#[usage(long)]
	pub process_log_objects: bool,

	#[usage(flatten)]
	pub process_output_objects: ProcessOutputObjects,

	/// Grant public read access.
	#[usage(long)]
	pub public: bool,

	#[usage(required = true)]
	pub references: Vec<tg::Reference>,

	#[usage(long)]
	pub sandbox_processes: bool,

	#[usage(flatten)]
	pub tag_targets: TagTargets,

	#[usage(long)]
	pub user_children: bool,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Eager {
	#[usage(
		default_missing = "true",
		long = "eager",
		overrides("--lazy"),
		require_equals = true
	)]
	eager: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "lazy",
		overrides("--eager"),
		require_equals = true
	)]
	lazy: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct ProcessErrorObjects {
	#[usage(
		default_missing = "true",
		long = "no-process-error-objects",
		overrides("--process-error-objects"),
		require_equals = true
	)]
	no_process_error_objects: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "process-error-objects",
		overrides("--no-process-error-objects"),
		require_equals = true
	)]
	process_error_objects: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct ProcessOutputObjects {
	#[usage(
		default_missing = "true",
		long = "no-process-output-objects",
		overrides("--process-output-objects"),
		require_equals = true
	)]
	no_process_output_objects: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "process-output-objects",
		overrides("--no-process-output-objects"),
		require_equals = true
	)]
	process_output_objects: Option<bool>,
}

#[derive(Clone, Debug, Default, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct TagTargets {
	#[usage(
		default_missing = "true",
		long = "no-tag-targets",
		overrides("--tag-targets"),
		require_equals = true
	)]
	no_tag_targets: Option<bool>,

	#[usage(
		default_missing = "true",
		long = "tag-targets",
		overrides("--no-tag-targets"),
		require_equals = true
	)]
	tag_targets: Option<bool>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let destination = args.destination.to_location()?;
	let source = tg::Location::Local(tg::location::Local::default());

	// Get the references.
	let location = Some(source.clone().into());
	let references = args
		.references
		.iter()
		.map(|reference| {
			let mut options = reference.options().clone();
			options.location.clone_from(&location);
			tg::Reference::new(
				reference.node().clone(),
				options,
				reference.export().map(ToOwned::to_owned),
			)
		})
		.collect::<Vec<_>>();
	let mut nodes = Vec::with_capacity(references.len());
	for reference in &references {
		let referent = tangram_cli_reference::get(cli, reference).await?.referent;
		let node = referent.try_map(|node| match node {
			tg::get::Node::Id(id) => Ok(id),
			tg::get::Node::Pointer(_) => Err(tg::error!("expected a node id")),
		})?;
		nodes.push(node);
	}

	// Push the nodes.
	let arg = tg::push::Arg {
		ancestors: args.ancestors.get(),
		destination: destination.clone(),
		eager: args.eager.get(),
		force: args.force,
		group_children: args.group_children,
		nodes,
		metadata: args.metadata,
		organization_children: args.organization_children,
		process_children: args.process_children,
		process_command_objects: args.process_command_objects,
		process_error_objects: args.process_error_objects.get(),
		process_log_objects: args.process_log_objects,
		process_output_objects: args.process_output_objects.get(),
		public: args.public,
		sandbox_processes: args.sandbox_processes,
		source: Some(source),
		tag_targets: args.tag_targets.get(),
		user_children: args.user_children,
	};
	let (header, stream) = client
		.push(arg)
		.await
		.map_err(|error| tg::error!(!error, "failed to push"))?;
	for node in &header.nodes {
		cli.print_info_referent(node, &args.print);
	}
	let output = cli.render_progress_stream(stream).await?;

	print_push_or_pull_amounts(cli, "skipped", &output.skipped);
	print_push_or_pull_amounts(cli, "transferred", &output.transferred);
	for referent in &output.nodes {
		Cli::print_referent(referent, &args.print);
	}

	Ok(())
}

pub fn print_push_or_pull_amounts(cli: &Cli, action: &str, amounts: &tg::push::Amounts) {
	let mut values = [
		(amounts.users, "user", "users"),
		(amounts.organizations, "organization", "organizations"),
		(amounts.groups, "group", "groups"),
		(amounts.tags, "tag", "tags"),
		(amounts.sandboxes, "sandbox", "sandboxes"),
		(amounts.processes, "process", "processes"),
		(amounts.objects, "object", "objects"),
	]
	.into_iter()
	.filter(|(amount, _, _)| *amount > 0)
	.map(|(amount, singular, plural)| {
		let name = if amount == 1 { singular } else { plural };
		format!("{amount} {name}")
	})
	.collect::<Vec<_>>();
	if amounts.bytes > 0 {
		let bytes = byte_unit::Byte::from_u64(amounts.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		values.push(format!("{bytes:#.1}"));
	}
	if values.is_empty() {
		return;
	}
	let message = format!("{action} {}", values.join(", "));
	cli.print_info_message(&message);
}

impl Eager {
	#[must_use]
	pub fn get(&self) -> bool {
		self.eager.or(self.lazy.map(|v| !v)).unwrap_or(true)
	}
}

impl ProcessErrorObjects {
	#[must_use]
	pub fn get(&self) -> bool {
		self.process_error_objects
			.or(self.no_process_error_objects.map(|value| !value))
			.unwrap_or(true)
	}
}

impl ProcessOutputObjects {
	#[must_use]
	pub fn get(&self) -> bool {
		self.process_output_objects
			.or(self.no_process_output_objects.map(|value| !value))
			.unwrap_or(true)
	}
}

impl TagTargets {
	#[must_use]
	pub fn get(&self) -> bool {
		self.tag_targets
			.or(self.no_tag_targets.map(|value| !value))
			.unwrap_or(true)
	}
}
