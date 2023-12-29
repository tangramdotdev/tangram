use crate::{
	tui::{self, Tui},
	Cli,
};
use std::{os::unix::process::CommandExt, path::PathBuf};
use tangram_client as tg;
use tangram_error::{Result, Wrap, WrapErr};

/// Build the specified target from a package and execute a command from its output.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
#[command(trailing_var_arg = true)]
pub struct Args {
	/// The path to the executable in the artifact to run.
	#[arg(long)]
	pub executable_path: Option<tg::Path>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[arg(long, default_value = "false")]
	pub no_tui: bool,

	/// The package to build.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[arg(default_value = "default")]
	pub target: String,

	/// Arguments to pass to the executable.
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_run(&self, args: Args) -> Result<()> {
		let tg = self.handle().await?;
		let tg = tg.as_ref();

		// Canonicalize the path.
		let mut package = args.package;
		if let Some(path) = package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.wrap_err("Failed to canonicalize the path.")?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(tg, &package).await?;

		// Create the target.
		let env = [(
			"TANGRAM_HOST".to_owned(),
			tg::System::host()?.to_string().into(),
		)]
		.into();
		let args_ = Vec::new();
		let host = tg::System::js();
		let path = tg::package::ROOT_MODULE_FILE_NAME.to_owned().into();
		let executable = tg::Symlink::new(Some(package.into()), path).into();
		let target = tg::target::Builder::new(host, executable)
			.lock(lock)
			.name(args.target.clone())
			.env(env)
			.args(args_)
			.build();

		// Print the target ID.
		eprintln!("{}", target.id(tg).await?);

		// Build the target.
		let build = target.build(tg, None, 0, args.retry).await?;

		// Print the build ID.
		eprintln!("{}", build.id());

		// Create the TUI.
		let tui = !args.no_tui;
		let tui = if tui {
			Tui::start(tg, &build, tui::Options::default()).await.ok()
		} else {
			None
		};

		// Wait for the build's outcome.
		let outcome = build.outcome(tg).await;

		// Stop the TUI.
		if let Some(tui) = tui {
			tui.stop();
			tui.join().await?;
		}

		// Handle for an error that occurred while waiting for the build's outcome.
		let outcome = outcome.wrap_err("Failed to get the build outcome.")?;

		// Handle a failed build.
		let output = outcome.into_result().wrap_err("The build failed.")?;

		// Get the output artifact.
		let artifact: tg::Artifact = output
			.try_into()
			.wrap_err("Expected the output to be an artifact.")?;

		// Get the path to the artifact.
		let artifact_path: PathBuf = self
			.path
			.join("artifacts")
			.join(artifact.id(tg).await?.to_string());

		// Get the executable path.
		let executable_path = if let Some(executable_path) = args.executable_path {
			// Resolve the argument as a path relative to the artifact.
			artifact_path.join(PathBuf::from(executable_path.to_string()))
		} else {
			match artifact {
				// If the artifact is a file or symlink, then the executable path should be the artifact itself.
				tg::artifact::Artifact::File(_) | tg::artifact::Artifact::Symlink(_) => {
					artifact_path
				},

				// If the artifact is a directory, then the executable path should be `.tangram/run`.
				tg::artifact::Artifact::Directory(_) => artifact_path.join(".tangram/run"),
			}
		};

		// Exec.
		Err(std::process::Command::new(executable_path)
			.args(args.trailing)
			.exec()
			.wrap("Failed to execute the command."))
	}
}
