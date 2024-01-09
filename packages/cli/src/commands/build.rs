use crate::{
	tui::{self, Tui},
	Cli,
};
use std::path::PathBuf;
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::package::Ext;

/// Build a target.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// If this flag is set, then the command will exit immediately instead of waiting for the build's output.
	#[arg(short, long, conflicts_with = "output")]
	pub detach: bool,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// Disable the TUI.
	#[arg(long, default_value = "false")]
	pub no_tui: bool,

	/// The path to check out the output to.
	#[arg(short, long)]
	pub output: Option<PathBuf>,

	/// The package to build.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[arg(default_value = "default")]
	pub target: String,
}

impl Cli {
	pub async fn command_build(&self, args: Args) -> Result<()> {
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
		let path = package.root_module_path(tg).await?;
		let executable = tg::Symlink::new(Some(package.into()), Some(path.to_string())).into();
		let target = tg::target::Builder::new(host, executable)
			.lock(lock)
			.name(args.target.clone())
			.env(env)
			.args(args_)
			.build();

		// Print the target ID.
		eprintln!("{}", target.id(tg).await?);

		// Build the target.
		let options = tg::build::Options {
			retry: args.retry,
			..Default::default()
		};
		let build = tg::Build::new(tg, target, options).await?;

		// If the detach flag is set, then exit.
		if args.detach {
			println!("{}", build.id());
			return Ok(());
		}

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

		// Check out the output if requested.
		if let Some(path) = args.output {
			let artifact = tg::Artifact::try_from(output.clone())
				.wrap_err("Expected the output to be an artifact.")?;
			artifact
				.check_out(tg, &path.try_into()?)
				.await
				.wrap_err("Failed to check out the artifact.")?;
		}

		// Print the output.
		println!("{output}");

		Ok(())
	}
}
