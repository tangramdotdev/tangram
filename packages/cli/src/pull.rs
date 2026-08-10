use {crate::Cli, tangram_client::prelude::*};

/// Pull items.
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
	pub tag_items: crate::push::TagItems,

	#[arg(long)]
	pub user_children: bool,
}

impl Cli {
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let source = args.source.to_location()?;

		// Get the references.
		let reference_location = source.clone().unwrap_or_else(|| {
			tg::Location::Remote(tg::location::Remote {
				name: "default".to_owned(),
				region: None,
			})
		});
		let reference_location = Some(reference_location.into());
		let references = args
			.references
			.iter()
			.map(|reference| {
				let mut options = reference.options().clone();
				if options.location.is_none() {
					options.location.clone_from(&reference_location);
				}
				tg::Reference::new(
					reference.item().clone(),
					options,
					reference.export().map(ToOwned::to_owned),
				)
			})
			.collect::<Vec<_>>();
		let mut items = Vec::with_capacity(references.len());
		for reference in &references {
			let referent = self.get(reference).await?.referent;
			let tg::get::Item::Id(id) = referent.item else {
				return Err(tg::error!("expected an item id"));
			};
			let item = tg::Referent::with_item_and_token(id, referent.options.token);
			items.push(item);
		}

		// Pull the items.
		let arg = tg::pull::Arg {
			ancestors: args.ancestors.get(),
			destination: None,
			eager: args.eager.get(),
			group_children: args.group_children,
			items,
			metadata: args.metadata,
			organization_children: args.organization_children,
			process_children: args.process_children,
			process_commands: args.process_commands,
			process_errors: args.process_errors,
			process_logs: args.process_logs,
			process_outputs: args.process_outputs.get(),
			sandbox_processes: args.sandbox_processes,
			source,
			tag_items: args.tag_items.get(),
			user_children: args.user_children,
		};
		let stream = client
			.pull(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to pull"))?;
		let output = self.render_progress_stream(stream).await?;

		let groups = output.skipped.groups;
		let objects = output.skipped.objects;
		let organizations = output.skipped.organizations;
		let processes = output.skipped.processes;
		let sandboxes = output.skipped.sandboxes;
		let tags = output.skipped.tags;
		let users = output.skipped.users;
		let bytes = byte_unit::Byte::from_u64(output.skipped.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!(
			"skipped {users} users, {organizations} organizations, {groups} groups, {tags} tags, {sandboxes} sandboxes, {processes} processes, {objects} objects, {bytes:#.1}"
		);
		self.print_info_message(&message);
		let groups = output.transferred.groups;
		let objects = output.transferred.objects;
		let organizations = output.transferred.organizations;
		let processes = output.transferred.processes;
		let sandboxes = output.transferred.sandboxes;
		let tags = output.transferred.tags;
		let users = output.transferred.users;
		let bytes = byte_unit::Byte::from_u64(output.transferred.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!(
			"transferred {users} users, {organizations} organizations, {groups} groups, {tags} tags, {sandboxes} sandboxes, {processes} processes, {objects} objects, {bytes:#.1}"
		);
		self.print_info_message(&message);

		Ok(())
	}
}
