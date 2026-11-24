use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Pull nodes.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub ancestors: tangram_cli_core::node::Options,

	#[usage(flatten)]
	pub eager: tangram_cli_push::Eager,

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
	pub process_error_objects: tangram_cli_push::ProcessErrorObjects,

	#[usage(long)]
	pub process_log_objects: bool,

	#[usage(flatten)]
	pub process_output_objects: tangram_cli_push::ProcessOutputObjects,

	#[usage(required = true)]
	pub references: Vec<tg::Reference>,

	#[usage(long)]
	pub sandbox_processes: bool,

	#[usage(flatten)]
	pub source: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub tag_targets: tangram_cli_push::TagTargets,

	#[usage(long)]
	pub user_children: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
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
		let referent = tangram_cli_reference::get(cli, reference).await?.referent;
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
		cli.print_info_referent(node, &args.print);
	}
	let output = cli.render_progress_stream(stream).await?;

	tangram_cli_push::print_push_or_pull_amounts(cli, "skipped", &output.skipped);
	tangram_cli_push::print_push_or_pull_amounts(cli, "transferred", &output.transferred);
	for referent in &output.nodes {
		Cli::print_referent(referent, &args.print);
	}

	Ok(())
}
