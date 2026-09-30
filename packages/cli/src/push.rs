use {crate::Cli, tangram_client::prelude::*};

/// Push nodes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub ancestors: crate::node::Options,

	#[command(flatten)]
	pub destination: crate::location::Args,

	#[command(flatten)]
	pub eager: Eager,

	/// Replace conflicting named nodes and their descendants.
	#[arg(long, short)]
	pub force: bool,

	#[arg(long)]
	pub group_children: bool,

	#[arg(long)]
	pub metadata: bool,

	#[arg(long)]
	pub organization_children: bool,

	#[arg(long)]
	pub process_children: bool,

	#[arg(long)]
	pub process_command_objects: bool,

	#[command(flatten)]
	pub process_error_objects: ProcessErrorObjects,

	#[arg(long)]
	pub process_log_objects: bool,

	#[command(flatten)]
	pub process_output_objects: ProcessOutputObjects,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[arg(long)]
	pub sandbox_processes: bool,

	#[command(flatten)]
	pub tag_targets: TagTargets,

	#[arg(long)]
	pub user_children: bool,
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct Eager {
	#[arg(
		default_missing_value = "true",
		id = "push.eager.eager",
		long = "eager",
		num_args = 0..=1,
		overrides_with = "push.eager.lazy",
		require_equals = true,
	)]
	eager: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.eager.lazy",
		long = "lazy",
		num_args = 0..=1,
		overrides_with = "push.eager.eager",
		require_equals = true,
	)]
	lazy: Option<bool>,
}

impl Eager {
	pub fn get(&self) -> bool {
		self.eager.or(self.lazy.map(|v| !v)).unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct ProcessErrorObjects {
	#[arg(
		default_missing_value = "true",
		id = "push.process_error_objects.no_process_error_objects",
		long = "no-process-error-objects",
		num_args = 0..=1,
		overrides_with = "push.process_error_objects.process_error_objects",
		require_equals = true,
	)]
	no_process_error_objects: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.process_error_objects.process_error_objects",
		long = "process-error-objects",
		num_args = 0..=1,
		overrides_with = "push.process_error_objects.no_process_error_objects",
		require_equals = true,
	)]
	process_error_objects: Option<bool>,
}

impl ProcessErrorObjects {
	pub fn get(&self) -> bool {
		self.process_error_objects
			.or(self.no_process_error_objects.map(|value| !value))
			.unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct ProcessOutputObjects {
	#[arg(
		default_missing_value = "true",
		id = "push.process_output_objects.no_process_output_objects",
		long = "no-process-output-objects",
		num_args = 0..=1,
		overrides_with = "push.process_output_objects.process_output_objects",
		require_equals = true,
	)]
	no_process_output_objects: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.process_output_objects.process_output_objects",
		long = "process-output-objects",
		num_args = 0..=1,
		overrides_with = "push.process_output_objects.no_process_output_objects",
		require_equals = true,
	)]
	process_output_objects: Option<bool>,
}

impl ProcessOutputObjects {
	pub fn get(&self) -> bool {
		self.process_output_objects
			.or(self.no_process_output_objects.map(|value| !value))
			.unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct TagTargets {
	#[arg(
		default_missing_value = "true",
		id = "push.tag_targets.no_tag_targets",
		long = "no-tag-targets",
		num_args = 0..=1,
		overrides_with = "push.tag_targets.tag_targets",
		require_equals = true,
	)]
	no_tag_targets: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.tag_targets.tag_targets",
		long = "tag-targets",
		num_args = 0..=1,
		overrides_with = "push.tag_targets.no_tag_targets",
		require_equals = true,
	)]
	tag_targets: Option<bool>,
}

impl TagTargets {
	pub fn get(&self) -> bool {
		self.tag_targets
			.or(self.no_tag_targets.map(|value| !value))
			.unwrap_or(true)
	}
}

impl Cli {
	pub async fn command_push(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let destination = args
			.destination
			.to_location()?
			.or(tg::push::Arg::default().destination);
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
			let referent = self.get(reference).await?.referent;
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
			sandbox_processes: args.sandbox_processes,
			source: Some(source),
			tag_targets: args.tag_targets.get(),
			user_children: args.user_children,
		};
		let stream = client
			.push(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to push"))?;
		let output = self.render_progress_stream(stream).await?;

		self.print_push_or_pull_amounts("skipped", &output.skipped);
		self.print_push_or_pull_amounts("transferred", &output.transferred);
		for node in &output.nodes {
			println!("{node}");
		}

		Ok(())
	}

	pub(crate) fn print_push_or_pull_amounts(&self, action: &str, amounts: &tg::push::Amounts) {
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
		self.print_info_message(&message);
	}
}
