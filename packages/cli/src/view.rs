use crate::Cli;
use tangram_client::{self as tg, handle::Ext as _};
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

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long, hide = true)]
	pub print: bool,

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
		let (referent, lockfile) = self.get_reference(&args.reference).await?;
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
		let (item, source_map) = match item {
			Either::Left(build) => {
				let source_map = if let Some(lockfile) = handle.get_build(build.id()).await?.lock {
					// Only attempt to load the target if the lock is set.
					if let Some(executable) =
						&*build.target(&handle).await?.executable(&handle).await?
					{
						let object = executable.object()[0].clone();
						let source_map = tg::SourceMap::new(&handle, &lockfile, object).await?;
						Some(source_map)
					} else {
						None
					}
				} else {
					None
				};
				(crate::viewer::Item::Build(build), source_map)
			},
			Either::Right(object) => {
				// Get the source map.
				let source_map = if let Some(lockfile) = lockfile {
					let lockfile = tg::Lockfile::try_read(&lockfile).await?.ok_or_else(
						|| tg::error!(%path = lockfile.display(), "failed to read lockfile"),
					)?;
					let source_map = tg::SourceMap::new(&handle, &lockfile, object.clone()).await?;
					Some(source_map)
				} else {
					None
				};
				(crate::viewer::Item::Value(object.into()), source_map)
			},
		};

		// Run the view.
		let kind = args.kind;
		let print = args.print;
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
						condensed_builds: false,
						expand_on_create: matches!(kind, Kind::Inline),
					};
					let mut viewer = crate::viewer::Viewer::new(&handle, item, options, source_map);
					match kind {
						Kind::Inline => {
							viewer.run_inline(stop).await?;
						},
						Kind::Fullscreen => {
							viewer.run_fullscreen(stop).await?;
						},
					}
					if print {
						println!("{}", viewer.tree().display());
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
