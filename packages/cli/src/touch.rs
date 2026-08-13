use {crate::Cli, tangram_client::prelude::*};

/// Touch an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_touch(&mut self, args: Args) -> tg::Result<()> {
		let reference_location = args.reference.options().location.clone();
		let mut options = args.reference.options().clone();
		if let Some(location) = args.locations.get() {
			options.location = Some(location);
		}
		let reference =
			tg::Reference::with_node_and_options(args.reference.node().clone(), options);

		let referent = self.resolve(&reference).await?;
		let locations = args
			.locations
			.with_fallback_location(referent.options.location.as_ref())
			.with_fallback_location_arg(reference_location);
		let is_process = matches!(
			referent.node(),
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let options = process.options.into();
			let process = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(process.node.into()),
				options,
			);
			let args = crate::process::touch::Args { locations, process };
			self.command_process_touch(args).await?;
		} else {
			let object = referent.try_map::<tg::object::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => Err(tg::error!("expected an object or process id")),
			})?;
			let options = object.options.into();
			let object = tg::Reference::with_node_and_options(
				tg::reference::Node::Id(object.node.into()),
				options,
			);
			let args = crate::object::touch::Args { locations, object };
			self.command_object_touch(args).await?;
		}

		Ok(())
	}
}
