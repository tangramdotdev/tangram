use crate::Cli;
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;

/// View a process or an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The maximum depth to render.
	#[arg(long)]
	pub depth: Option<u32>,

	/// Choose the kind of view, either inline or fullscreen.
	#[arg(default_value = "fullscreen", long)]
	pub kind: Kind,

	/// If this flag is set, the lock will not be updated.
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
	pub async fn command_view(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item() {
			Either::Left(process) => crate::viewer::Item::Process(process.clone()),
			Either::Right(object) => crate::viewer::Item::Value(object.clone().into()),
		};
		let root = referent.map(|_| item);

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
						auto_expand_and_collapse_processes: false,
						show_process_commands: true,
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
