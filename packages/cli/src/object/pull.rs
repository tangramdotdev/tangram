use crate::Cli;
use futures::StreamExt as _;
use tangram_client::{self as tg, Handle as _};

/// Pull an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_pull(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());

		// Pull the object.
		let arg = tg::object::pull::Arg { remote };
		let mut stream = handle.pull_object(&args.object, arg).await?.boxed();

		// Create the progress bar.
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		// Update the progress bars.
		while let Some(result) = stream.next().await {
			match result {
				Ok(tg::object::push::Event::Progress(progress)) => {
					objects_progress_bar.set_position(progress.count.current);
					if let Some(total) = progress.count.total {
						objects_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						objects_progress_bar.set_length(total);
					}
					bytes_progress_bar.set_position(progress.weight.current);
					if let Some(total) = progress.weight.total {
						bytes_progress_bar.set_style(indicatif::ProgressStyle::default_bar());
						bytes_progress_bar.set_length(total);
					}
				},
				Ok(tg::object::push::Event::End) => {
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
