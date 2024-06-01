use crate::Cli;
use futures::{StreamExt as _, TryStreamExt};
use tangram_client as tg;
use tg::Handle as _;

/// Push an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,

	#[arg(short, long)]
	pub remote: Option<String>,
}

impl Cli {
	pub async fn command_object_push(&self, args: Args) -> tg::Result<()> {
		// Push the object.
		let remote = args.remote.unwrap_or_else(|| "default".to_owned());
		let arg = tg::object::push::Arg { remote };
		let mut stream = self.handle.push_object(&args.object, arg).await?.boxed();

		// Create the progress bar.
		let count_progress_bar = indicatif::ProgressBar::new_spinner();
		let weight_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(count_progress_bar.clone());
		progress_bar.add(weight_progress_bar.clone());

		// Update the progress bars.
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::object::push::Event::Progress(progress) => {
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
				tg::object::push::Event::End => {
					break;
				},
			}
		}

		// Clear the progress bar.
		progress_bar.clear().unwrap();

		Ok(())
	}
}
