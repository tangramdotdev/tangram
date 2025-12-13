use {crate::Cli, futures::future, tangram_client::prelude::*};

pub type Args = crate::push::Args;

impl Cli {
	pub async fn command_pull(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the references.
		let referents = self.get_references(&args.references).await?;
		let items = referents
			.into_iter()
			.map(|referent| {
				referent
					.item
					.map_left(|process| process.id().clone())
					.map_right(|object| object.id().clone())
			})
			.collect::<Vec<_>>();

		// Pull the items.
		let arg = tg::pull::Arg {
			commands: args.commands,
			eager: args.eager.get(),
			items: items.clone(),
			logs: args.logs,
			outputs: args.outputs.get(),
			recursive: args.recursive,
			remote: Some(args.remote),
		};
		let stream = handle.pull(arg).await?;
		let output = self.render_progress_stream(stream).await?;
		let bytes = byte_unit::Byte::from_u64(output.bytes)
			.get_appropriate_unit(byte_unit::UnitType::Decimal);
		let message = format!(
			"pulled {} processes, {} objects, {bytes:#.1}",
			output.processes, output.objects,
		);
		Self::print_info_message(&message);

		// Put tags.
		future::try_join_all(std::iter::zip(&args.references, &items).map(
			async |(reference, item)| {
				if let tg::reference::Item::Tag(pattern) = reference.item()
					&& let Ok(tag) = pattern.clone().try_into()
				{
					let arg = tg::tag::put::Arg {
						force: args.force,
						item: item.clone(),
						local: None,
						remotes: None,
					};
					handle.put_tag(&tag, arg).await?;
				}
				Ok::<_, tg::Error>(())
			},
		))
		.await?;
		for (reference, item) in std::iter::zip(&args.references, &items) {
			if reference.item().is_tag() {
				let message = format!("tagged {reference} {item}");
				Self::print_info_message(&message);
			}
		}

		Ok(())
	}
}
