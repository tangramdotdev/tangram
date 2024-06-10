use crate::Cli;
use std::path::PathBuf;
use futures::TryStreamExt;
use tangram_client as tg;
use tg::Handle;
use std::pin::pin;

/// Check out an artifact.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The artifact to check out.
	pub artifact: tg::artifact::Id,

	/// Whether to bundle the artifact before checkout.
	#[arg(long)]
	pub bundle: bool,

	/// Whether to overwrite an existing file system object at the path.
	#[arg(short, long, requires = "path")]
	pub force: bool,

	/// The path to check out the artifact to. The default is the artifact's ID in the checkouts directory.
	pub path: Option<PathBuf>,

	/// Whether to check out the artifact's references.
	#[arg(long, default_value_t = true)]
	pub references: bool,
}

impl Cli {
	pub async fn command_artifact_checkout(&self, args: Args) -> tg::Result<()> {
		// Get the path.
		let path = if let Some(path) = args.path {
			let current = std::env::current_dir()
				.map_err(|source| tg::error!(!source, "failed to get the working directory"))?;
			let path = current.join(&path);
			let parent = path
				.parent()
				.ok_or_else(|| tg::error!("the path must have a parent directory"))?;
			let file_name = path
				.file_name()
				.ok_or_else(|| tg::error!("the path must have a file name"))?;
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the parent directory"))?;
			let path = parent
				.canonicalize()
				.map_err(|source| tg::error!(!source, "failed to canonicalize the path"))?
				.join(file_name);
			let path = path.try_into()?;
			Some(path)
		} else {
			None
		};

		// Create the progress bars.
		let objects_progress_bar = indicatif::ProgressBar::new_spinner();
		let bytes_progress_bar = indicatif::ProgressBar::new_spinner();

		let progress_bar = indicatif::MultiProgress::new();
		progress_bar.add(objects_progress_bar.clone());
		progress_bar.add(bytes_progress_bar.clone());

		// Create the arg.
		let arg = tg::artifact::checkout::Arg {
			bundle: path.is_some(),
			force: args.force,
			path,
			references: true,
		};

		// Check out the artifact.
		let stream = self
			.handle
			.check_out_artifact(&args.artifact, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create check out stream"))?;
		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			match event {
				tg::artifact::checkout::Event::Progress(progress) => {
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
				tg::artifact::checkout::Event::End(path) => {
					progress_bar.clear().unwrap();
					println!("{path}");
					break;
				}
			}
		}

		Ok(())
	}
}
