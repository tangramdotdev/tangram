use crate::Cli;
use futures::{StreamExt as _, TryStreamExt};
use tangram_client as tg;
use tg::Handle as _;

/// Push an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub object: tg::object::Id,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,
}

impl Cli {
	pub async fn command_object_push(&self, args: Args) -> tg::Result<()> {
		// Get the object's metadata.
		let metadata = self.handle.get_object_metadata(&args.object).await?;

		// Push the object.
		let remote = args
			.remote
			.map(|remote| remote.unwrap_or_else(|| "default".to_owned()));
		let arg = tg::object::push::Arg { remote };
		let mut stream = self.handle.push_object(&args.object, arg).await?.boxed();

		// Create the progress bar.
		let count_progress_bar = indicatif::ProgressBar::new(metadata.count.unwrap_or(0));
		let weight_progress_bar = indicatif::ProgressBar::new(metadata.weight.unwrap_or(0));
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(count_progress_bar.clone());
		progress_bar.add(weight_progress_bar.clone());

		// Update the progress bars.
		while let Some(progress) = stream.try_next().await? {
			count_progress_bar.set_position(progress.current_count);
			count_progress_bar.set_length(progress.total_count);
			weight_progress_bar.set_position(progress.current_weight);
			weight_progress_bar.set_length(progress.total_weight);
		}

		// Clear the progress bar.
		progress_bar.clear().unwrap();

		Ok(())
	}
}
