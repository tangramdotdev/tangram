use crate::Cli;
use std::os::unix::process::CommandExt as _;
use tangram_client as tg;
use tg::Handle as _;

/// Build a target and run a command.
#[derive(Debug, clap::Args)]
#[clap(trailing_var_arg = true)]
pub struct Args {
	#[clap(flatten)]
	pub inner: super::build::GetOrCreateInnerArgs,

	/// The path to the executable in the artifact to run.
	#[clap(short = 'x', long)]
	pub executable: Option<tg::Path>,

	/// Arguments to pass to the executable.
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_run(&self, args: Args) -> tg::Result<()> {
		let output = self
			.command_build_get_or_create_inner(args.inner, false)
			.await?;

		// Get the path to the artifact.
		let mut artifact_path = match output.unwrap() {
			super::build::GetOrCreateInnerOutput::Path(path) => path,
			super::build::GetOrCreateInnerOutput::Value(value) => {
				let artifact: tg::Artifact = value.try_into().map_err(|source| {
					tg::error!(!source, "expected the output to be an artifact")
				})?;
				self.handle
					.path()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the server path"))?
					.ok_or_else(|| tg::error!("failed to get the server path"))?
					.join("artifacts")
					.join(artifact.id(&self.handle, None).await?.to_string())
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
		let error = std::process::Command::new(executable_path.as_path())
			.args(args.trailing)
			.exec();
		Err(tg::error!(source = error, %executable_path, "failed to execute the command"))
	}
}
