use crate::Cli;
use std::os::unix::process::CommandExt as _;
use tangram_client::{self as tg, Handle as _};

/// Spawn and await a sandboxed process, then exec an executable from its output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::process::build::Options,

	/// The path to the executable in the artifact to run.
	#[arg(short = 'x', long)]
	pub executable: Option<std::path::PathBuf>,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Arguments to pass to the executable.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_process_exec(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Build.
		let output = self
			.build_process(args.build, reference, vec![])
			.await?
			.unwrap();

		// Check out the output.
		let artifact: tg::Artifact = output
			.try_into()
			.map_err(|source| tg::error!(!source, "expected the output to be an artifact"))?;
		let arg = tg::artifact::checkout::Arg {
			dependencies: false,
			force: false,
			lockfile: false,
			path: None,
		};
		let stream = handle
			.check_out_artifact(&artifact.id(&handle).await?, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the checkout stream"))?;
		let output = self.render_progress_stream(stream).await?;
		let artifact_path = output.path;

		// Get the executable path.
		let executable_path = if let Some(executable_path) = args.executable {
			// Resolve the argument as a path relative to the artifact.
			artifact_path.join(executable_path)
		} else {
			// Ensure the path is not a directory.
			let metadata = tokio::fs::metadata(&artifact_path)
				.await
				.map_err(|source| tg::error!(!source, %artifact_path  = artifact_path.display(), "failed to stat the artifact"))?;
			if metadata.is_dir() {
				return Err(
					tg::error!(%artifact_path = artifact_path.display(), "cannot execute a directory"),
				);
			}
			artifact_path
		};

		// Exec.
		let error = std::process::Command::new(&executable_path)
			.args(args.trailing)
			.exec();
		Err(
			tg::error!(source = error, %executable_path = executable_path.display(), "failed to execute the command"),
		)
	}
}
