use crate::{
	tui::{self, Tui},
	Cli,
};
use std::os::unix::process::CommandExt;
use tangram_client as tg;
use tangram_error::{error, Result};

/// Build the specified target from a package and execute a command from its output.
#[derive(Debug, clap::Args)]
#[clap(trailing_var_arg = true)]
pub struct Args {
	/// The path to the executable in the artifact to run.
	#[clap(short = 'x', long)]
	pub executable: Option<tg::Path>,

	/// If this flag is set, then the package's lockfile will not be updated.
	#[clap(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[clap(long, default_value = "false")]
	pub no_tui: bool,

	/// The package to build.
	#[clap(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[clap(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[clap(short, long, default_value = "default")]
	pub target: String,

	/// Arguments to pass to the executable.
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_run(&self, mut args: Args) -> Result<()> {
		let client = &self.client().await?;

		// Canonicalize the path.
		if let Some(path) = args.package.path.as_mut() {
			*path = tokio::fs::canonicalize(&path)
				.await
				.map_err(|error| error!(source = error, %path, "failed to canonicalize the path"))?
				.try_into()?;
		}

		// Create the package.
		let (package, lock) = tg::package::get_with_lock(client, &args.package).await?;

		// Create the target.
		let env = [(
			"TANGRAM_HOST".to_owned(),
			tg::Triple::host()?.to_string().into(),
		)]
		.into();
		let args_ = Vec::new();
		let host = tg::Triple::js();
		let path = tg::package::get_root_module_path(client, &package).await?;
		let executable = tg::Symlink::new(Some(package.into()), Some(path.to_string())).into();
		let target = tg::target::Builder::new(host, executable)
			.lock(lock)
			.name(args.target.clone())
			.env(env)
			.args(args_)
			.build();

		// Print the target ID.
		eprintln!("{}", target.id(client).await?);

		// Build the target.
		let arg = tg::build::GetOrCreateArg {
			parent: None,
			remote: false,
			retry: args.retry,
			target: target.id(client).await?.clone(),
		};
		let build = tg::Build::new(client, arg).await?;

		// Print the build ID.
		eprintln!("{}", build.id());

		// Attempt to get the build's outcome with zero timeout.
		let arg = tg::build::outcome::GetArg {
			timeout: Some(std::time::Duration::ZERO),
		};
		let outcome = build
			.get_outcome(client, arg)
			.await
			.map_err(|error| error!(source = error, "failed to get the build outcome"))?;

		// If the outcome is not immediatey available, then wait for it while showing the TUI if enabled.
		let outcome = if let Some(outcome) = outcome {
			outcome
		} else {
			// Create the TUI.
			let tui = !args.no_tui;
			let tui = if tui {
				Tui::start(client, &build, tui::Options::default())
					.await
					.ok()
			} else {
				None
			};

			// Wait for the build's outcome.
			let outcome = build.outcome(client).await;

			// Stop the TUI.
			if let Some(tui) = tui {
				tui.stop();
				tui.join().await?;
			}

			outcome.map_err(|error| error!(source = error, "failed to get the build outcome"))?
		};

		// Handle a failed build.
		let output = outcome
			.into_result()
			.map_err(|error| error!(source = error, "the build failed"))?;

		// Get the output artifact.
		let artifact: tg::Artifact = output
			.try_into()
			.map_err(|error| error!(source = error, "expected the output to be an artifact"))?;

		// Get the path to the artifact.
		let artifact_path = client
			.path()
			.await
			.map_err(|error| error!(source = error, "failed to get the server path"))?
			.ok_or_else(|| error!("failed to get the server path"))?
			.join("artifacts")
			.join(artifact.id(client).await?.to_string());

		// Get the executable path.
		let executable_path = if let Some(executable_path) = args.executable {
			// Resolve the argument as a path relative to the artifact.
			artifact_path.join(executable_path)
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
		let error = std::process::Command::new(&executable_path)
			.args(args.trailing)
			.exec();
		Err(error!(source = error, %executable_path, "failed to execute the command"))
	}
}
