use {crate::Cli, std::collections::HashSet, tangram_client::prelude::*};

/// Get a package's outdated dependencies.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(default_value = ".", index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_outdated(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let mut visitor = Visitor::default();
		let referent = self
			.get_reference(&args.reference)
			.await?
			.try_map(|item| item.left().ok_or_else(|| tg::error!("expected an object")))?;
		tg::object::visit(&handle, &mut visitor, &referent)
			.await
			.map_err(|source| tg::error!(!source, "failed to walk objects"))?;
		let output = visitor.entries.into_iter().collect::<Vec<_>>();
		self.print_serde(output, args.print).await?;
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
	async fn visit_directory(
		&mut self,
		_handle: &H,
		_directory: tg::Referent<&tg::Directory>,
	) -> tg::Result<bool> {
		Ok(true)
	}

	async fn visit_symlink(
		&mut self,
		_handle: &H,
		_symlink: tg::Referent<&tg::Symlink>,
	) -> tg::Result<bool> {
		Ok(true)
	}

	async fn visit_file(&mut self, handle: &H, file: tg::Referent<&tg::File>) -> tg::Result<bool> {
		let file = file.item;

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

		Ok(true)
	}
}
