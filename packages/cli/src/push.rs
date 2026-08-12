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

	#[arg(long)]
	pub group_children: bool,

	#[arg(long)]
	pub metadata: bool,

	#[arg(long)]
	pub organization_children: bool,

	#[arg(long)]
	pub process_children: bool,

	#[arg(alias = "process-command", long)]
	pub process_commands: bool,

	#[arg(alias = "process-error", long)]
	pub process_errors: bool,

	#[arg(alias = "process-log", long)]
	pub process_logs: bool,

	#[command(flatten)]
	pub process_outputs: ProcessOutputs,

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
pub struct ProcessOutputs {
	#[arg(
		alias = "process-output",
		default_missing_value = "true",
		id = "push.process_outputs.process_outputs",
		long = "process-outputs",
		num_args = 0..=1,
		overrides_with = "push.process_outputs.no_process_outputs",
		require_equals = true,
	)]
	process_outputs: Option<bool>,

	#[arg(
		alias = "no-process-output",
		default_missing_value = "true",
		id = "push.process_outputs.no_process_outputs",
		long = "no-process-outputs",
		num_args = 0..=1,
		overrides_with = "push.process_outputs.process_outputs",
		require_equals = true,
	)]
	no_process_outputs: Option<bool>,
}

impl ProcessOutputs {
	pub fn get(&self) -> bool {
		self.process_outputs
			.or(self.no_process_outputs.map(|value| !value))
			.unwrap_or(true)
	}
}

#[derive(Clone, Debug, Default, clap::Args)]
pub struct TagTargets {
	#[arg(
		default_missing_value = "true",
		id = "push.tag_targets.tag_targets",
		long = "tag-targets",
		num_args = 0..=1,
		overrides_with = "push.tag_targets.no_tag_targets",
		require_equals = true,
	)]
	tag_targets: Option<bool>,

	#[arg(
		default_missing_value = "true",
		id = "push.tag_targets.no_tag_targets",
		long = "no-tag-targets",
		num_args = 0..=1,
		overrides_with = "push.tag_targets.tag_targets",
		require_equals = true,
	)]
	no_tag_targets: Option<bool>,
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
		let destination = args.destination.to_location()?;
		let source = tg::Location::Local(tg::location::Local::default());

		// Get the references.
		let reference_location = Some(source.clone().into());
		let references = args
			.references
			.iter()
			.map(|reference| {
				let mut options = reference.options().clone();
				options.location.clone_from(&reference_location);
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
			let tg::get::Node::Id(id) = referent.node else {
				return Err(tg::error!("expected a node id"));
			};
			let node = tg::Referent::with_node_and_token(id, referent.options.token);
			nodes.push(node);
		}

		// Push the nodes.
		let arg = tg::push::Arg {
			ancestors: args.ancestors.get(),
			destination: destination.clone(),
			eager: args.eager.get(),
			group_children: args.group_children,
			nodes,
			metadata: args.metadata,
			organization_children: args.organization_children,
			process_children: args.process_children,
			process_commands: args.process_commands,
			process_errors: args.process_errors,
			process_logs: args.process_logs,
			process_outputs: args.process_outputs.get(),
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

		Ok(())
	}

	pub(crate) fn print_push_or_pull_amounts(&self, action: &str, amounts: &tg::push::Amounts) {
		let mut values = [
			(amounts.users, "users"),
			(amounts.organizations, "organizations"),
			(amounts.groups, "groups"),
			(amounts.tags, "tags"),
			(amounts.sandboxes, "sandboxes"),
			(amounts.processes, "processes"),
			(amounts.objects, "objects"),
		]
		.into_iter()
		.filter(|(amount, _)| *amount > 0)
		.map(|(amount, name)| format!("{amount} {name}"))
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
