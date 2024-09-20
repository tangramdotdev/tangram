use crate::Cli;
use std::{os::unix::process::CommandExt as _, path::PathBuf};
use tangram_client as tg;

/// Build a target and run a command.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::target::build::Args,

	/// The path to the executable in the artifact to run.
	#[arg(short = 'x', long)]
	pub executable: Option<std::path::PathBuf>,

	/// Arguments to pass to the executable.
	#[arg(index = 1, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_target_run(&self, mut args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Check out the output.
		args.build.checkout = Some(None);

		// Build the target.
		let output = self.command_target_build_inner(args.build).await?;

		// Get the path to the artifact.
		let mut artifact_path = match output {
			crate::target::build::InnerOutput::Detached(_) => unreachable!(),
			crate::target::build::InnerOutput::Path(path) => path,
			crate::target::build::InnerOutput::Value(value) => {
				let artifact: tg::Artifact = value.try_into().map_err(|source| {
					tg::error!(!source, "expected the output to be an artifact")
				})?;
				let path = self
					.config
					.as_ref()
					.and_then(|config| config.path.clone())
					.unwrap_or_else(|| {
						PathBuf::from(std::env::var("HOME").unwrap()).join(".tangram")
					});
				path.join("artifacts")
					.join(artifact.id(&handle).await?.to_string())
			},
		};

		// Get the executable path.
		let executable_path = if let Some(executable_path) = args.executable {
			// Resolve the argument as a path relative to the artifact.
			artifact_path.join(executable_path)
		} else {
			// If the artifact is a directory, then the executable path should be `.tangram/run`.
			let metadata = tokio::fs::metadata(&artifact_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to stat the artifact"))?;
			if metadata.is_dir() {
				artifact_path = artifact_path.join(".tangram/run");
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
