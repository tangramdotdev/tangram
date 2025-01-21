use crate::Cli;
use std::{os::unix::process::CommandExt as _, path::PathBuf};
use tangram_client as tg;

/// Build a command and exec an executable from its output.
#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path to the executable in the artifact to run.
	#[arg(short = 'x', long)]
	pub executable: Option<std::path::PathBuf>,

	#[command(flatten)]
	pub inner: crate::command::run::InnerArgs,

	/// The reference to the command to build.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Arguments to pass to the executable.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_command_exec(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let reference = args
			.reference
			.clone()
			.unwrap_or_else(|| ".".parse().unwrap());

		// Run the command.
		let kind = crate::command::run::InnerKind::Build;
		let output = self.command_run_inner(reference, kind, args.inner).await?;

		// Get the path to the artifact.
		let mut artifact_path = match output {
			crate::command::run::InnerOutput::Detached(_) => unreachable!(),
			crate::command::run::InnerOutput::Path(path) => path,
			crate::command::run::InnerOutput::Value(value) => {
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
