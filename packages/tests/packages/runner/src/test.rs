use {
	crate::{Result, database, semaphore, snapshot, temp, vfs},
	std::{
		fmt::Write as _,
		fs::File,
		path::{Path, PathBuf},
		process::Stdio,
		sync::Arc,
		time::{Duration, Instant},
	},
	tokio::process::Command,
};

/// The exit code that indicates a skipped test.
pub const SKIPPED: i32 = 77;

/// The time to wait for the supervisor to clean up after it is asked to stop.
const STOP: Duration = Duration::from_secs(15);

/// A test file.
#[derive(Clone, Debug)]
pub struct Test {
	pub name: String,
	pub path: PathBuf,
}

/// The options that apply to every test.
#[derive(Clone, Debug)]
pub struct Options {
	pub cloud: bool,
	pub cluster_path: Option<PathBuf>,
	pub database_run: Option<String>,
	pub fskit: bool,
	pub no_capture: bool,
	pub offline: bool,
	pub preserve_failing_temps: bool,
	pub preserve_temps: bool,
	pub quickjs: bool,
	pub stress: bool,
	pub timeout: Duration,
	pub turso: bool,
}

/// The result of running a test.
#[derive(Clone, Debug)]
pub struct Output {
	pub cleanup_duration: Duration,
	pub code: i32,
	pub duration: Duration,
	pub execution_duration: Duration,
	pub log: String,
	pub name: String,
	pub skip_reason: Option<String>,
	pub stop: Option<Stop>,
	pub temp_path: Option<PathBuf>,
}

/// The reason that the runner stopped a test.
#[derive(Clone, Copy, Debug)]
pub enum Stop {
	Interrupted,
	TimedOut,
}

/// Run a test. The stop receiver resolves when the run is interrupted.
pub async fn run(
	test: Test,
	options: Arc<Options>,
	mut stop: tokio::sync::watch::Receiver<bool>,
) -> Output {
	let start = Instant::now();
	let result = run_inner(&test, &options, &mut stop, start).await;
	match result {
		Ok(output) => output,
		Err(error) => Output {
			cleanup_duration: Duration::ZERO,
			code: 1,
			duration: start.elapsed(),
			execution_duration: Duration::ZERO,
			log: format!("error: {error}\n"),
			name: test.name,
			skip_reason: None,
			stop: None,
			temp_path: None,
		},
	}
}

#[must_use]
pub fn fskit_temp_root() -> PathBuf {
	PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".tangram/test-tmp")
}

impl Output {
	#[must_use]
	pub fn failed(&self) -> bool {
		self.code != 0 && self.code != SKIPPED
	}
}

async fn run_inner(
	test: &Test,
	options: &Options,
	stop: &mut tokio::sync::watch::Receiver<bool>,
	start: Instant,
) -> Result<Output> {
	// Create the temporary directory. With FSKit, it must live under the Tangram directory, because that is the only path outside the app group container that the file system extension's sandbox permits.
	let parent = if options.fskit {
		fskit_temp_root()
	} else {
		std::env::temp_dir()
	};
	let temp_path = tangram_tests_util::Temp::with_parent(&parent)?.into_path();
	let working_path = temp_path.join("work");
	std::fs::create_dir(&working_path)?;

	// Remove the snapshot state from a previous run. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	if !options.stress {
		snapshot::prepare(test);
	}

	// Write the config.
	let config = config(options);
	if let Some(config) = config {
		std::fs::write(temp_path.join("config.json"), serde_json::to_vec(&config)?)?;
	}

	// Spawn the test under the supervisor.
	let library_path = crate::repository_path().join("packages/tests/util/test.nu");
	let script = [
		format!("use {} cleanup_background_jobs", nuon(&library_path)),
		"$env.config.display_errors.exit_code = true".to_owned(),
		"try {".to_owned(),
		format!("source {}", nuon(&test.path)),
		"} finally {".to_owned(),
		format!("cleanup_background_jobs {}", nuon(&temp_path)),
		"}".to_owned(),
	]
	.join("\n");
	let output_path = temp_path.join("output");
	let leaks_path = temp_path.join("leaks");
	let mut command = Command::new("tangram_tests_supervisor");
	command
		.arg("run")
		.arg("--leaks")
		.arg(&leaks_path)
		.arg("--path")
		.arg(&temp_path)
		.args(["--", "nu", "--no-config-file", "--commands", &script])
		.current_dir(&working_path)
		.envs(environment(options, &temp_path))
		.stdin(Stdio::null());
	if options.no_capture {
		command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
	} else {
		let file = File::options()
			.append(true)
			.create(true)
			.open(&output_path)?;
		command.stdout(file.try_clone()?).stderr(file);
	}
	let mut child = command.spawn()?;

	// Wait for the test to exit, time out, or be interrupted.
	let (code, stop) = tokio::select! {
		status = child.wait() => (code(status?), None),
		() = tokio::time::sleep(options.timeout) => {
			terminate(&mut child).await;
			(124, Some(Stop::TimedOut))
		},
		_ = async { stop.wait_for(|stop| *stop).await.map(|_| ()) } => {
			terminate(&mut child).await;
			(130, Some(Stop::Interrupted))
		},
	};
	let execution_duration = start.elapsed();

	// Clean up.
	let cleanup_start = Instant::now();
	let mut errors = cleanup(test, options, &temp_path, code).await;

	// Read the output and the skip reason.
	let mut output = std::fs::read_to_string(&output_path).unwrap_or_default();
	let skip_reason = (code == SKIPPED)
		.then(|| std::fs::read_to_string(temp_path.join("skip")).ok())
		.flatten()
		.map(|reason| reason.trim().to_owned());
	if matches!(stop, Some(Stop::TimedOut)) {
		writeln!(output, "the test timed out after {:?}", options.timeout).unwrap();
	}

	// Remove the temporary directory unless it is preserved.
	let failed = code != 0 && code != SKIPPED;
	let preserve =
		!errors.is_empty() || options.preserve_temps || (options.preserve_failing_temps && failed);
	if !preserve {
		let path = temp_path.clone();
		let result =
			tokio::task::spawn_blocking(move || tangram_tests_util::temp::remove(&path)).await?;
		if let Err(error) = result {
			errors.push(error.to_string());
		}
	}

	// Report the cleanup errors as a failure.
	let code = if errors.is_empty() {
		code
	} else {
		for error in &errors {
			writeln!(output, "cleanup: {error}").unwrap();
		}
		if code == 0 || code == SKIPPED {
			1
		} else {
			code
		}
	};

	let output = Output {
		cleanup_duration: cleanup_start.elapsed(),
		code,
		duration: start.elapsed(),
		execution_duration,
		log: output,
		name: test.name.clone(),
		skip_reason,
		stop,
		temp_path: Some(temp_path),
	};

	Ok(output)
}

/// Clean up after a test, and return the errors.
async fn cleanup(test: &Test, options: &Options, temp_path: &Path, code: i32) -> Vec<String> {
	let mut errors = Vec::new();

	// Report the processes that outlived the test.
	if let Ok(leaks) = std::fs::read_to_string(temp_path.join("leaks")) {
		let leaks = leaks.lines().collect::<Vec<_>>().join(", ");
		errors.push(format!(
			"terminated test processes that survived cleanup: {leaks}"
		));
	}

	// Unmount the VFS, then unlink the semaphores while the lock files still exist.
	if let Err(error) = vfs::unmount(temp_path, options.fskit).await {
		errors.push(error.to_string());
	}
	let path = temp_path.to_owned();
	match tokio::task::spawn_blocking(move || semaphore::cleanup(&path)).await {
		Ok(Ok(())) => (),
		Ok(Err(error)) => errors.push(format!("failed to unlink the test semaphores: {error}")),
		Err(error) => errors.push(error.to_string()),
	}

	// Prune the snapshots of a passing test. Skip this in stress mode, because concurrent runs of the same test would race on these files.
	if code == 0 && !options.stress {
		snapshot::prune(test);
	}

	// Reset the cloud database instances.
	if let (true, Some(cluster_path), Some(run)) =
		(options.cloud, &options.cluster_path, &options.database_run)
	{
		let instances = std::fs::read_to_string(temp_path.join("instances")).unwrap_or_default();
		let mut instances = instances
			.lines()
			.filter(|line| !line.is_empty())
			.collect::<Vec<_>>();
		instances.sort_unstable();
		instances.dedup();
		let results = futures::future::join_all(
			instances
				.iter()
				.map(|instance| database::reset(instance, cluster_path, run)),
		)
		.await;
		for result in results {
			if let Err(error) = result {
				errors.push(error.to_string());
			}
		}
	}

	errors
}

fn code(status: std::process::ExitStatus) -> i32 {
	use std::os::unix::process::ExitStatusExt as _;
	status
		.code()
		.or_else(|| status.signal().map(|signal| 128 + signal))
		.unwrap_or(1)
}

fn config(options: &Options) -> Option<serde_json::Value> {
	let mut config = serde_json::Map::new();
	if options.preserve_failing_temps || options.preserve_temps {
		config.insert(
			"advanced".to_owned(),
			serde_json::json!({ "preserve_temp_directories": true }),
		);
	}
	(!config.is_empty()).then_some(serde_json::Value::Object(config))
}

fn environment(options: &Options, temp_path: &Path) -> Vec<(String, String)> {
	let flag = |value: bool| if value { "1" } else { "" }.to_owned();
	let path = |value: &Option<PathBuf>| {
		value
			.as_ref()
			.map(|path| path.display().to_string())
			.unwrap_or_default()
	};
	vec![
		("SHELL".to_owned(), "/bin/sh".to_owned()),
		(
			"TANGRAM_CONFIG".to_owned(),
			temp_path.join("config.json").display().to_string(),
		),
		("TANGRAM_MODE".to_owned(), "client".to_owned()),
		("TANGRAM_QUIET".to_owned(), "true".to_owned()),
		("TANGRAM_TEST_CLOUD".to_owned(), flag(options.cloud)),
		(
			"TANGRAM_TEST_DATABASE_POOL".to_owned(),
			if options.cloud {
				database::pool_path().display().to_string()
			} else {
				String::new()
			},
		),
		(
			"TANGRAM_TEST_DATABASE_RUN".to_owned(),
			options.database_run.clone().unwrap_or_default(),
		),
		(
			"TANGRAM_TEST_FDB_CLUSTER".to_owned(),
			path(&options.cluster_path),
		),
		("TANGRAM_TEST_FSKIT".to_owned(), flag(options.fskit)),
		("TANGRAM_TEST_OFFLINE".to_owned(), flag(options.offline)),
		("TANGRAM_TEST_QUICKJS".to_owned(), flag(options.quickjs)),
		(
			"TANGRAM_TEST_SKIP_PATH".to_owned(),
			temp_path.join("skip").display().to_string(),
		),
		("TANGRAM_TEST_TURSO".to_owned(), flag(options.turso)),
		("TMPDIR".to_owned(), temp_path.display().to_string()),
	]
}

/// Quote a path as a Nushell string literal.
fn nuon(path: &Path) -> String {
	serde_json::to_string(&path.display().to_string()).unwrap()
}

/// Ask the supervisor to terminate the test, and kill it if it does not exit in time.
async fn terminate(child: &mut tokio::process::Child) {
	if let Some(pid) = child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()) {
		// SAFETY: kill has no memory safety preconditions.
		unsafe {
			libc::kill(pid, libc::SIGTERM);
		}
	}
	if tokio::time::timeout(STOP, child.wait()).await.is_err() {
		child.kill().await.ok();
	}
}
