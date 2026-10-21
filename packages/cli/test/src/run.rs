use {
	crate::{
		Result, database,
		progress::{self, Counts, Progress, Running},
		snapshot,
		test::{self, Options, Output, SKIPPED, Test},
	},
	std::{
		collections::{BTreeMap, VecDeque},
		path::PathBuf,
		process::ExitCode,
		sync::Arc,
		time::{Duration, Instant},
	},
	tokio::task::JoinSet,
};

/// Run the tests whose paths match the filters.
#[derive(clap::Args)]
#[expect(clippy::struct_excessive_bools)]
pub struct Args {
	/// Accept all new and updated snapshots.
	#[arg(long, short)]
	accept: bool,

	/// Use the shared cloud databases for cloud test instances.
	#[arg(long)]
	cloud: bool,

	/// Regular expressions matched against each test's path.
	filters: Vec<String>,

	/// Use `FSKit` for the VFS.
	#[arg(long)]
	fskit: bool,

	/// The number of concurrent tests to run.
	#[arg(long, short)]
	jobs: Option<usize>,

	/// The path to the Linux kernel image to use with --vm.
	#[arg(long)]
	kernel_path: Option<PathBuf>,

	/// Do not capture the output of each test. This sets --jobs to 1.
	#[arg(long)]
	no_capture: bool,

	/// Show only the aggregate progress bar, without listing running tests.
	#[arg(long)]
	no_progress_details: bool,

	/// Skip tests which require network access.
	#[arg(long)]
	offline: bool,

	/// Keep the temporary directories for failed tests.
	#[arg(long)]
	preserve_failing_temps: bool,

	/// Keep the temporary directories.
	#[arg(long)]
	preserve_temps: bool,

	/// Print the output of passing tests.
	#[arg(long)]
	print_passing_test_output: bool,

	/// Use `QuickJS` as the JavaScript engine.
	#[arg(long)]
	quickjs: bool,

	/// Review snapshots.
	#[arg(long, short)]
	review: bool,

	/// Run the matching tests repeatedly until one fails.
	#[arg(long)]
	stress: bool,

	/// Run the matching tests this many times, then stop. Implies --stress.
	#[arg(long)]
	stress_count: Option<usize>,

	/// The timeout for each test in seconds.
	#[arg(default_value_t = 120, long)]
	timeout: u64,

	/// Use Turso for the server database.
	#[arg(long)]
	turso: bool,

	/// Run every test against the VFS.
	#[arg(long)]
	vfs: bool,

	/// Use VM isolation as the default for the test harness.
	#[arg(long)]
	vm: bool,
}

/// A test waiting to run.
struct Entry {
	round: usize,
	test: Test,
}

pub async fn run(args: Args) -> Result<ExitCode> {
	// Validate the arguments.
	let stress = args.stress || args.stress_count.is_some();
	if stress && (args.accept || args.review) {
		return Err("--stress may not be combined with --accept or --review".into());
	}
	if args.stress_count == Some(0) {
		return Err("--stress-count must be at least one".into());
	}
	if args.jobs == Some(0) {
		return Err("--jobs must be at least one".into());
	}
	if args.timeout == 0 {
		return Err("--timeout must be greater than zero".into());
	}
	if args.vm && args.kernel_path.is_none() {
		return Err("--kernel-path is required when --vm is set".into());
	}

	// Select the tests.
	let tests = discover(&args.filters)?;
	if tests.is_empty() {
		return Err("no tests matched the provided filters".into());
	}

	// Check the cloud databases and reclaim the leases of exited runs.
	let cluster_path = if args.cloud {
		database::check().await?;
		database::reclaim().await?;
		Some(database::cluster_path()?)
	} else {
		None
	};

	// Create the options.
	let options = Arc::new(Options {
		cloud: args.cloud,
		cluster_path,
		fskit: args.fskit,
		kernel_path: args.kernel_path.clone(),
		no_capture: args.no_capture,
		offline: args.offline,
		preserve_failing_temps: args.preserve_failing_temps,
		preserve_temps: args.preserve_temps,
		quickjs: args.quickjs,
		stress,
		timeout: Duration::from_secs(args.timeout),
		turso: args.turso,
		vfs: args.vfs,
		vm: args.vm,
	});

	// Run the tests.
	let jobs = if args.no_capture {
		1
	} else {
		args.jobs.unwrap_or_else(|| {
			std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
		})
	};
	let progress = Progress::new(
		!args.no_progress_details,
		!args.no_capture,
		args.print_passing_test_output,
	);
	let (results, interrupted) =
		schedule(&tests, &options, jobs, args.stress_count, progress).await;

	// Accept or review the snapshots.
	if args.accept {
		snapshot::accept(&tests)?;
	}
	if args.review {
		snapshot::review(&tests)?;
	}

	// Print the summary.
	let passed = results.iter().filter(|output| output.code == 0).count();
	let skipped = results
		.iter()
		.filter(|output| output.code == SKIPPED)
		.count();
	let failed = results
		.iter()
		.filter(|output| output.failed())
		.collect::<Vec<_>>();
	eprintln!(
		"\x1b[32m{passed} passed\x1b[0m, \x1b[33m{skipped} skipped\x1b[0m, \x1b[31m{} failed\x1b[0m, {} total",
		failed.len(),
		results.len()
	);
	for output in &failed {
		eprintln!(
			"\x1b[31m✗\x1b[0m {} {}, exit {} ({})",
			output.name,
			progress::duration(output.duration),
			output.code,
			progress::describe(output)
		);
	}
	let preserved = results
		.iter()
		.filter(|output| {
			options.preserve_temps || (options.preserve_failing_temps && output.failed())
		})
		.filter_map(|output| output.temp_path.as_ref().map(|path| (&output.name, path)))
		.collect::<Vec<_>>();
	if !preserved.is_empty() {
		eprintln!();
		eprintln!("preserved temp directories:");
		for (name, path) in preserved {
			eprintln!("  {name}: {}", path.display());
		}
	}

	let code = if interrupted {
		ExitCode::from(130)
	} else if failed.is_empty() {
		ExitCode::SUCCESS
	} else {
		ExitCode::FAILURE
	};

	Ok(code)
}

/// Find the test files whose paths match any of the filters.
fn discover(filters: &[String]) -> Result<Vec<Test>> {
	let pattern = if filters.is_empty() {
		".*".to_owned()
	} else {
		filters
			.iter()
			.map(|filter| format!("({filter})"))
			.collect::<Vec<_>>()
			.join("|")
	};
	let regex = regex::Regex::new(&pattern)?;
	let tests_path = std::fs::canonicalize(crate::repository_path().join("packages/cli/tests"))?;
	let mut paths = Vec::new();
	crate::temp::walk(&tests_path, &mut |path| {
		if path.extension().is_some_and(|extension| extension == "nu") {
			paths.push(path.to_owned());
		}
	});
	let mut tests = paths
		.into_iter()
		.filter_map(|path| {
			let name = path.strip_prefix(&tests_path).ok()?.to_str()?.to_owned();
			if name.starts_with("lib/") || !regex.is_match(path.to_str()?) {
				return None;
			}
			Some(Test { name, path })
		})
		.collect::<Vec<_>>();
	tests.sort_by(|a, b| a.path.cmp(&b.path));
	Ok(tests)
}

/// Run the tests with a pool of workers. Return the results and whether the run was interrupted.
async fn schedule(
	tests: &[Test],
	options: &Arc<Options>,
	jobs: usize,
	stress_count: Option<usize>,
	mut progress: Progress,
) -> (Vec<Output>, bool) {
	// Create the state.
	let mut pending = tests
		.iter()
		.map(|test| Entry {
			round: 1,
			test: test.clone(),
		})
		.collect::<VecDeque<_>>();
	let total = if options.stress {
		stress_count.map(|count| count * tests.len())
	} else {
		Some(tests.len())
	};
	let mut round = 1;
	let mut sequence = 0u64;
	let mut stopped = false;
	let mut interrupted = false;
	let mut running = BTreeMap::<u64, (String, Instant)>::new();
	let mut results = Vec::new();
	let mut tasks = JoinSet::new();
	let (stop_sender, stop_receiver) = tokio::sync::watch::channel(false);
	let mut frames = tokio::time::interval(progress::FRAME);
	frames.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	let ctrl_c = tokio::signal::ctrl_c();
	tokio::pin!(ctrl_c);

	loop {
		// Keep the pool full. In stress mode, refill the queue with the next round as necessary, so the pool stays full even when fewer tests match than there are jobs.
		while !stopped && running.len() < jobs {
			if pending.is_empty()
				&& options.stress
				&& stress_count.is_none_or(|count| round < count)
			{
				round += 1;
				pending.extend(tests.iter().map(|test| Entry {
					round,
					test: test.clone(),
				}));
			}
			let Some(entry) = pending.pop_front() else {
				break;
			};
			sequence += 1;
			running.insert(sequence, (entry.test.name.clone(), Instant::now()));
			let options = options.clone();
			let stop = stop_receiver.clone();
			let id = sequence;
			tasks.spawn(async move {
				let output = test::run(entry.test, options, stop).await;
				(id, entry.round, output)
			});
		}
		if tasks.is_empty() {
			break;
		}

		// Wait for a test to finish, the next frame, or an interrupt.
		tokio::select! {
			Some(result) = tasks.join_next() => {
				let Ok((id, round, output)) = result else {
					continue;
				};
				running.remove(&id);
				progress.result(&output);
				if options.stress && output.failed() {
					let message = format!("\x1b[31m{} failed on round {round}\x1b[0m\n", output.name);
					progress.message(&message);
					stopped = true;
				}
				results.push(output);
				continue;
			},
			_ = frames.tick() => (),
			_ = &mut ctrl_c, if !interrupted => {
				interrupted = true;
				stopped = true;
				stop_sender.send(true).ok();
				continue;
			},
		}

		// Render the frame.
		let running = running
			.values()
			.map(|(name, start)| Running {
				name,
				start: *start,
			})
			.collect::<Vec<_>>();
		let counts = Counts {
			completed: results.len(),
			failed: results.iter().filter(|output| output.failed()).count(),
			passed: results.iter().filter(|output| output.code == 0).count(),
			skipped: results
				.iter()
				.filter(|output| output.code == SKIPPED)
				.count(),
			total,
		};
		progress.render(&running, &counts);
	}
	progress.finish();

	(results, interrupted)
}
