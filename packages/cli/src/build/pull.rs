use crate::Cli;
use futures::{StreamExt as _, TryStreamExt as _};
use tangram_client as tg;
use tg::Handle as _;

/// Pull a build.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,

	#[arg(long)]
	pub logs: bool,

	#[arg(long)]
	pub recursive: bool,

	#[arg(short, long)]
	pub remote: Option<String>,

	#[arg(long)]
	pub targets: bool,
}

impl Cli {
	pub async fn command_build_pull(&self, args: Args) -> tg::Result<()> {
		// Pull the build.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		let arg = tg::build::pull::Arg {
			logs: args.logs,
			recursive: args.recursive,
			remote,
			targets: args.targets,
		};
		let mut stream = self.handle.pull_build(&args.build, arg).await?.boxed();

		// Create the progress bar.
		let builds_progress_bar = indicatif::ProgressBar::new_spinner();
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(builds_progress_bar.clone());
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		// Update the progress bars.
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::build::pull::Event::Progress(progress) => {
					builds_progress_bar.set_position(progress.builds.current);
					if let Some(total) = progress.builds.total {
						builds_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						builds_progress_bar.set_length(total);
					}
					objects_progress_bar.set_position(progress.objects.current);
					if let Some(total) = progress.objects.total {
						objects_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						objects_progress_bar.set_length(total);
					}
					bytes_progress_bar.set_position(progress.bytes.current);
					if let Some(total) = progress.bytes.total {
						bytes_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						bytes_progress_bar.set_length(total);
					}
				},
				tg::build::pull::Event::End => {
					break;
				},
			}
		}

		// Clear the progress bar.
		progress_bar.clear().unwrap();

		Ok(())
	}
}
