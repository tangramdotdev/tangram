use {crate::Cli, tangram_client as tg, tangram_either::Either, tangram_futures::task::Task};

/// View a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	#[command(flatten)]
	pub expand: ExpandOptions,

	/// Choose the kind of view, either inline or fullscreen.
	#[arg(default_value = "fullscreen", long)]
	pub kind: Kind,

	/// If this flag is set, the lock will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// If set, view the reference as a tag, package, or value.
	#[arg(long = "mode", default_value = "value")]
	pub mode: Mode,

	/// The reference to view.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, clap::Args)]
pub struct ExpandOptions {
	/// Auto collapse process children when processes finish.
	#[arg(long)]
	pub collapse_process_children: bool,

	/// Auto expand objects.
	#[arg(long = "expand-value")]
	pub object: bool,

	/// Auto expand package nodes.
	#[arg(long = "expand-package")]
	pub package: bool,

	/// Auto expand process nodes.
	#[arg(long = "expand-process")]
	pub process: bool,

	/// Auto expand tag nodes.
	#[arg(long = "expand-tag")]
	pub tag: bool,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	Inline,
	#[default]
	Fullscreen,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum Mode {
	Tag,
	Package,
	Value,
}

impl Cli {
	pub async fn command_view(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let root = match args.mode {
			Mode::Tag => {
				let item = args.reference.item();
				let pattern = item.clone().try_unwrap_tag().map_err(
					|source| tg::error!(!source, %reference = args.reference, "expected a tag"),
				)?;
				tg::Referent::with_item(crate::viewer::Item::Tag(pattern.clone()))
			},
			Mode::Value | Mode::Package => {
				let referent = self.get_reference(&args.reference).await?;
				let item = match (referent.item(), args.mode) {
					(Either::Left(_), Mode::Package) => {
						return Err(tg::error!(%reference = args.reference, "expected an object"));
					},
					(Either::Left(process), Mode::Value) => {
						crate::viewer::Item::Process(process.clone())
					},
					(Either::Right(object), Mode::Package) => {
						crate::viewer::Item::Package(crate::viewer::Package(object.clone()))
					},
					(Either::Right(object), Mode::Value) => {
						crate::viewer::Item::Value(object.clone().into())
					},
					(_, Mode::Tag) => unreachable!(),
				};
				referent.map(|_| item)
			},
		};

		let kind = args.kind;
		Task::spawn_blocking(move |stop| {
			let local_set = tokio::task::LocalSet::new();
			let runtime = tokio::runtime::Builder::new_current_thread()
				.worker_threads(1)
				.enable_all()
				.build()
				.unwrap();
			local_set
				.block_on(&runtime, async move {
					let options = crate::viewer::Options {
						expand: args.expand,
						show_process_commands: true,
						clear_at_end: !matches!(kind, Kind::Inline),
					};
					let mut viewer = crate::viewer::Viewer::new(&handle, root, options);
					match kind {
						Kind::Inline => {
							viewer.run_inline(stop).await?;
						},
						Kind::Fullscreen => {
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
