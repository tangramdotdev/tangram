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
		let count_progress_bar = indicatif::ProgressBar::new_spinner();
		let weight_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(count_progress_bar.clone());
		progress_bar.add(weight_progress_bar.clone());

		// Update the progress bars.
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::build::pull::Event::Progress(progress) => {
					count_progress_bar.set_position(progress.current_count);
					if let Some(total_count) = progress.total_count {
						count_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						count_progress_bar.set_length(total_count);
					}
					weight_progress_bar.set_position(progress.current_weight);
					if let Some(total_weight) = progress.total_weight {
						weight_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						weight_progress_bar.set_length(total_weight);
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
