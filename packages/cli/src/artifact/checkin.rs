use crate::Cli;
use crossterm::style::Stylize;
use futures::TryStreamExt as _;
use std::path::PathBuf;
use std::pin::pin;
use tangram_client as tg;
use tg::Handle;

/// Check in an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Destroy the artifact if requested.
	#[arg(short, long)]
	pub destructive: bool,

	/// The path to check in.
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_artifact_checkin(&self, args: Args) -> tg::Result<()> {
		// Get the path.
		let mut path = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
		if let Some(path_arg) = &args.path {
			path.push(path_arg);
		}

		// Perform the checkin.
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();

		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		let arg = tg::artifact::checkin::Arg {
			path: path.try_into()?,
			destructive: args.destructive,
		};

		let stream = self
			.handle
			.check_in_artifact(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create check in stream"))?;
		let mut stream = pin!(stream);

		while let Some(event) = stream.try_next().await? {
			match event {
				tg::artifact::checkin::Event::Progress(progress) => {
					objects_progress_bar.set_position(progress.count.current);
					objects_progress_bar.set_message(format!(
						"{}: {} ({})",
						"objects".blue(),
						progress.count.current,
						progress.path
					));
					bytes_progress_bar.set_position(progress.weight.current);
					bytes_progress_bar.set_message(format!(
						"{}: {}",
						"bytes".blue(),
						progress.weight.current,
					));
				},
				tg::artifact::checkin::Event::End(id) => {
					// Clear the progress bar.
					progress_bar.clear().unwrap();
					println!("{id}");
					break;
				},
			}
		}

		Ok(())
	}
}
