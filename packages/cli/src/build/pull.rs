use crate::Cli;
use futures::StreamExt as _;
use tangram_client as tg;

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
		let client = self.client().await?;

		// Pull the build.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		let arg = tg::build::pull::Arg {
			logs: args.logs,
			outcomes: true,
			recursive: args.recursive,
			remote,
			targets: args.targets,
		};
		let mut stream = client.pull_build(&args.build, arg).await?.boxed();

		// Create the progress bar.
		let builds_progress_bar = indicatif::ProgressBar::new_spinner();
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(builds_progress_bar.clone());
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		// Update the progress bars.
		while let Some(event) = stream.next().await {
			match event {
				Ok(tg::build::pull::Event::Progress(progress)) => {
					builds_progress_bar.set_position(progress.build_count.current);
					if let Some(total) = progress.build_count.total {
						builds_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						builds_progress_bar.set_length(total);
					}
					objects_progress_bar.set_position(progress.object_count.current);
					if let Some(total) = progress.object_count.total {
						objects_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						objects_progress_bar.set_length(total);
					}
					bytes_progress_bar.set_position(progress.object_weight.current);
					if let Some(total) = progress.object_weight.total {
						bytes_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						bytes_progress_bar.set_length(total);
					}
				},
				Ok(tg::build::pull::Event::End) => {
					progress_bar.clear().unwrap();
					break;
				},
				Err(error) => {
					progress_bar.clear().unwrap();
					return Err(error);
				},
			}
		}

		Ok(())
	}
}
