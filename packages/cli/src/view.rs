use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;

/// View a build or object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	/// Choose the kind of view, either inline or fullscreen.
	#[arg(long, default_value = "fullscreen")]
	pub kind: Kind,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The reference to view.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
	Inline,
	#[default]
	Fullscreen,
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(build) => Either::Left(build),
			Either::Right(object) => {
				let object = if let Some(subpath) = &referent.subpath {
					let directory = object
						.try_unwrap_directory()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?;
					directory.get(&handle, subpath).await?.into()
				} else {
					object
				};
				Either::Right(object)
			},
		};
		let item = match item {
			Either::Left(build) => crate::viewer::Item::Build(build),
			Either::Right(object) => crate::viewer::Item::Value(object.into()),
		};

		// Run the view.
		let kind = args.kind;
		Task::spawn_blocking(move |stop| {
			tokio::runtime::LocalRuntime::new()
				.unwrap()
				.block_on(async move {
					let options = crate::viewer::Options {
						collapse_finished_builds: false,
						expand_on_create: matches!(kind, Kind::Inline),
						hide_build_targets: false,
						max_depth: None,
					};
					let mut viewer = crate::viewer::Viewer::new(&handle, item, options);
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
