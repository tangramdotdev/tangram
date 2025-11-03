use {crate::Cli, tangram_client as tg, tangram_either::Either, tangram_futures::task::Task};

/// View a process, an object, or a tag.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Collapse process children when processes finish.
	#[arg(long)]
	pub collapse_process_children: bool,

	/// The maximum depth.
	#[arg(long)]
	pub depth: Option<u32>,

	/// Expand objects.
	#[arg(long)]
	pub expand_objects: bool,

	/// Expand packages.
	#[arg(long)]
	pub expand_packages: bool,

	/// Expand processs.
	#[arg(long)]
	pub expand_processes: bool,

	/// Expand tags.
	#[arg(long)]
	pub expand_tags: bool,

	/// Whether to view the item as a tag, package, or value.
	#[arg(long = "mode", default_value = "value")]
	pub kind: Kind,

	/// Choose the mode, either inline or fullscreen.
	#[arg(default_value = "fullscreen", long)]
	pub mode: Mode,

	/// The reference to view.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
	Inline,
	#[default]
	Fullscreen,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum Kind {
	Tag,
	Package,
	Value,
}

impl Cli {
	pub async fn command_view(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let root = match args.kind {
			Kind::Tag => {
				let item = args.reference.item();
				let pattern = item.clone().try_unwrap_tag().map_err(
					|source| tg::error!(!source, %reference = args.reference, "expected a tag"),
				)?;
				tg::Referent::with_item(crate::viewer::Item::Tag(pattern.clone()))
			},
			Kind::Value | Kind::Package => {
				let referent = self.get_reference(&args.reference).await?;
				let item = match (referent.item(), args.kind) {
					(Either::Left(_), Kind::Package) => {
						return Err(tg::error!(%reference = args.reference, "expected an object"));
					},
					(Either::Left(process), Kind::Value) => {
						crate::viewer::Item::Process(process.clone())
					},
					(Either::Right(object), Kind::Package) => {
						crate::viewer::Item::Package(crate::viewer::Package(object.clone()))
					},
					(Either::Right(object), Kind::Value) => {
						crate::viewer::Item::Value(object.clone().into())
					},
					(_, Kind::Tag) => unreachable!(),
				};
				referent.map(|_| item)
			},
		};

		let mode = args.mode;
		Task::spawn_blocking(move |stop| {
			let local_set = tokio::task::LocalSet::new();
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.unwrap();
			local_set
				.block_on(&runtime, async move {
					let options = crate::viewer::Options {
						collapse_process_children: args.collapse_process_children,
						depth: args.depth,
						expand_objects: args.expand_objects,
						expand_packages: args.expand_packages,
						expand_processes: args.expand_processes,
						expand_tags: args.expand_tags,
						show_process_commands: true,
					};
					let mut viewer = crate::viewer::Viewer::new(&handle, root, options);
					match mode {
						Mode::Inline => {
							viewer.run_inline(stop, true).await?;
						},
						Mode::Fullscreen => {
							viewer.run_fullscreen(stop).await?;
						},
					}
					Ok::<_, tg::Error>(())
				})
				.unwrap();
		})
		.wait()
		.await
		.unwrap();

		Ok(())
	}
}
