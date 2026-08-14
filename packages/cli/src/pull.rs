use {crate::Cli, tangram_client::prelude::*};

/// Pull nodes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub ancestors: crate::node::Options,

	#[command(flatten)]
	pub eager: crate::push::Eager,

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
	pub process_outputs: crate::push::ProcessOutputs,

	#[arg(required = true)]
	pub references: Vec<tg::Reference>,

	#[arg(long)]
	pub sandbox_processes: bool,

	#[command(flatten)]
	pub source: crate::location::Args,

	#[command(flatten)]
	pub tag_targets: crate::push::TagTargets,

	#[arg(long)]
	pub user_children: bool,
}

impl Cli {
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
			source,
			tag_targets: args.tag_targets.get(),
			user_children: args.user_children,
		};
		let stream = client
			.pull(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to pull"))?;
		let output = self.render_progress_stream(stream).await?;

		self.print_push_or_pull_amounts("skipped", &output.skipped);
		self.print_push_or_pull_amounts("transferred", &output.transferred);

		Ok(())
	}
}
