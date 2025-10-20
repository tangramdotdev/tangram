use {crate::Cli, std::collections::HashSet, tangram_client as tg};

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_outdated(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let object = self
			.get_reference(&args.reference)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference"))?
			.item()
			.clone()
			.right()
			.ok_or_else(|| tg::error!(%reference = args.reference, "expected an object"))?;
		let mut visitor = Visitor::default();
		tg::object::visit(&handle, &mut visitor, [object])
			.await
			.map_err(|source| tg::error!(!source, "failed to walk objects"))?;
		let output = visitor.entries.into_iter().collect::<Vec<_>>();
		Self::print_json(&output, None).await?;
		Ok(())
	}
}

#[derive(Default)]
struct Visitor {
	entries: HashSet<Entry>,
}

#[derive(serde::Serialize, Debug, PartialEq, Eq, Hash)]
struct Entry {
	current: tg::Tag,
	compatible: Option<tg::Tag>,
	latest: Option<tg::Tag>,
}

impl<H> tg::object::Visitor<H> for Visitor
where
	H: tg::Handle,
{
	async fn visit_file(
		&mut self,
		handle: &H,
		file: &tangram_client::File,
	) -> tangram_client::Result<()> {
		// Get the file's dependencies.
		let dependencies = file
			.dependencies(handle)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the file's dependencies"))?
			.into_iter()
			.filter_map(|(reference, referent)| {
				let pattern = reference.item().clone().try_unwrap_tag().ok()?;
				let tag = referent?.options.tag.take()?;
				Some((pattern, tag))
			});

		for (pattern, current) in dependencies {
			let compatible = handle
				.list_tags(tg::tag::list::Arg {
					length: Some(1),
					pattern: pattern.clone(),
					recursive: false,
					remote: None,
					reverse: true,
				})
				.await?
				.data
				.into_iter()
				.map(|o| o.tag)
				.next();

			let mut components = pattern.components().collect::<Vec<_>>();
			components.pop();
			components.push("*");
			let pattern = tg::tag::Pattern::new(components.join("/"));
			let latest = handle
				.list_tags(tg::tag::list::Arg {
					length: Some(1),
					pattern: pattern.clone(),
					recursive: false,
					remote: None,
					reverse: true,
				})
				.await?
				.data
				.into_iter()
				.map(|o| o.tag)
				.next();

			let entry = Entry {
				current,
				compatible,
				latest,
			};

			self.entries.insert(entry);
		}

		Ok(())
	}
}
