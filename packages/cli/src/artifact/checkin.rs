use crate::Cli;
use futures::StreamExt as _;
use std::path::PathBuf;
use tangram_client::{self as tg, Handle as _};

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Check in the artifact faster by allowing Tangram to move it.
	#[arg(long)]
	pub destructive: bool,

	/// If this flag is set, lockfiles will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The path to check in.
	#[arg(index = 1)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_artifact_checkin(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Check in the artifact.
		let arg = tg::artifact::checkin::Arg {
			destructive: args.destructive,
			locked: args.locked,
			path: path.try_into()?,
			dependencies: true,
		};
		let mut stream = handle
			.check_in_artifact(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create check in stream"))?
			.boxed();

		// Create the progress bar.
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();
		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		while let Some(event) = stream.next().await {
			match event {
				Ok(tg::artifact::checkin::Event::Progress(progress)) => {
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
				Ok(tg::artifact::checkin::Event::End(id)) => {
					progress_bar.clear().unwrap();
					println!("{id}");
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
