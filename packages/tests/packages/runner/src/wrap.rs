use {
	crate::{Result, database, semaphore},
	std::{path::PathBuf, process::ExitCode},
	tokio::{
		process::Command,
		signal::unix::{SignalKind, signal},
	},
};

/// Run a test under the test supervisor, which contains every process that the test starts, then clean up after it. Fail the test if any process outlived it or the cleanup fails.
#[derive(clap::Args)]
pub struct Args {
	/// The command that runs the test.
	#[arg(last = true, required = true)]
	pub command: Vec<String>,
}

pub async fn wrap(args: Args) -> Result<ExitCode> {
	// Outlive the signals that end the test, so that the cleanup below runs after the supervisor, or the client that runs it in a sandbox, stops the test.
	let _signals = [
		signal(SignalKind::hangup())?,
		signal(SignalKind::interrupt())?,
		signal(SignalKind::terminate())?,
	];

	// Create the test's temporary directory.
	let path = tangram_tests_util::Temp::new()?.into_path();

	// Get the tangram that runs the test in a Tangram sandbox, if any.
	let sandbox = std::env::var_os("TANGRAM_TEST_SANDBOX")
		.filter(|value| !value.is_empty())
		.map(PathBuf::from);

	// Take this run's lock on the database pool leases that the test acquires, and reclaim the leases of exited runs, unless the test runs in a sandbox, which cannot reach the cloud databases.
	let run = if sandbox.is_none() && database::available() {
		let run = database::Run::new(&database::pool_path())?;
		database::reclaim().await?;
		Some(run)
	} else {
		None
	};

	// Run the test under the supervisor, in a Tangram sandbox if requested.
	let semaphores = path.join(semaphore::MANIFEST).display().to_string();
	let mut command = if let Some(tangram) = &sandbox {
		// Pass the test's environment except the variables that the sandbox sets, those that `tg run` sets, and the cloud databases' variable, so that the cloud tests skip.
		let environment = std::env::vars_os()
			.filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
			.filter(|(key, _)| {
				!matches!(
					key.as_str(),
					"HOME"
						| "PATH" | "TANGRAM_TEST_CLOUD"
						| "TANGRAM_TEST_SANDBOX"
						| "TANGRAM_TEST_SEMAPHORES"
						| "TMPDIR"
				) && (!key.starts_with("TANGRAM_") || key.starts_with("TANGRAM_TEST_"))
			})
			.chain([
				("TANGRAM_TEST_SEMAPHORES".to_owned(), semaphores),
				("TMPDIR".to_owned(), path.display().to_string()),
			])
			.collect();
		let working_path = std::env::current_dir()?;
		crate::test::sandbox(tangram, true, &path, &working_path, environment)?
	} else {
		let mut command = Command::new("tangram_tests_supervisor");
		command
			.env("TANGRAM_TEST_SEMAPHORES", semaphores)
			.env("TMPDIR", &path);
		command
	};
	command
		.arg("run")
		.arg("--leaks")
		.arg(path.join("leaks"))
		.arg("--path")
		.arg(&path)
		.arg("--")
		.args(&args.command);
	if let Some(run) = &run {
		command.env("TANGRAM_TEST_DATABASE_RUN", run.id());
	}
	let status = command.status().await?;
	let code = crate::test::code(status);

	// Report the processes that outlived the test.
	let mut errors = Vec::new();
	if let Ok(leaks) = std::fs::read_to_string(path.join("leaks"))
		&& !leaks.trim().is_empty()
	{
		let leaks = leaks.lines().collect::<Vec<_>>().join(", ");
		errors.push(format!(
			"terminated test processes that survived cleanup: {leaks}"
		));
	}

	// Reset the database pool instances that the test leased, unlink its semaphores, and remove its temporary directory.
	if let Some(run) = &run {
		errors.extend(database::reset_recorded(&path, &database::cluster_path()?, run.id()).await);
	}
	if let Err(error) = semaphore::cleanup(&path) {
		errors.push(format!("failed to unlink the test semaphores: {error}"));
	}
	if std::env::var_os("TANGRAM_TEST_PRESERVE_TEMPS").is_some() {
		eprintln!("preserved the temporary directory {}", path.display());
	} else if let Err(error) = tangram_tests_util::temp::remove(&path) {
		errors.push(error.to_string());
	}

	// Fail the test if the cleanup failed.
	for error in &errors {
		eprintln!("{error}");
	}
	let code = if errors.is_empty() || code != 0 {
		code
	} else {
		1
	};
	let code = ExitCode::from(u8::try_from(code).unwrap_or(1));

	Ok(code)
}
