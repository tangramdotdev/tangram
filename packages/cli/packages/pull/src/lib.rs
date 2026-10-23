use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Pull nodes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub ancestors: tangram_cli_core::node::Options,

	#[command(flatten)]
	pub eager: tangram_cli_push::Eager,

	/// Replace conflicting named nodes and their descendants.
	#[arg(long, short)]
	pub force: bool,

	#[arg(long)]
	pub group_children: bool,

	#[arg(long)]
	pub metadata: bool,

	#[arg(long)]
	pub organization_children: bool,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(long)]
	pub process_children: bool,

	#[arg(long)]
	pub process_command_objects: bool,

	#[command(flatten)]
	pub process_error_objects: tangram_cli_push::ProcessErrorObjects,

	#[arg(long)]
	pub process_log_objects: bool,

	#[command(flatten)]
	pub process_output_objects: tangram_cli_push::ProcessOutputObjects,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[arg(long)]
	pub sandbox_processes: bool,

	#[command(flatten)]
	pub source: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub tag_targets: tangram_cli_push::TagTargets,

	#[arg(long)]
	pub user_children: bool,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let source = args.source.to_location()?;

		// Get the references.
		let location = source.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let location = Some(location.into());
		let references = args
			.references
			.iter()
			.map(|reference| {
				let mut options = reference.options().clone();
				if options.location.is_none() {
					options.location.clone_from(&location);
				}
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

		// Pull the nodes.
		let arg = tg::pull::Arg {
			ancestors: args.ancestors.get(),
			destination: None,
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
			source,
			tag_targets: args.tag_targets.get(),
			user_children: args.user_children,
		};
		let (header, stream) = client
			.pull(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to pull"))?;
		for node in &header.nodes {
			self.print_info_referent(node, &args.print);
		}
		let output = self.render_progress_stream(stream).await?;

		let push = tangram_cli_push::Component::new(self);
		push.print_push_or_pull_amounts("skipped", &output.skipped);
		push.print_push_or_pull_amounts("transferred", &output.transferred);
		for referent in &output.nodes {
			Cli::print_referent(referent, &args.print);
		}

		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
