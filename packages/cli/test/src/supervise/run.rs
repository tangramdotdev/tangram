use {
	crate::{Result, process, signal::Signals},
	std::{
		ffi::OsString,
		os::unix::process::CommandExt as _,
		path::PathBuf,
		process::Command,
		time::{Duration, Instant},
	},
};

/// The time that processes have to exit on their own after the test exits.
const GRACE: Duration = Duration::from_secs(2);

/// The time that processes have to exit after SIGKILL.
const KILL: Duration = Duration::from_secs(2);

/// The time that processes have to exit after SIGTERM.
const TERMINATE: Duration = Duration::from_secs(5);

/// Run a test and contain every process it starts.
#[derive(clap::Args)]
pub struct Args {
	/// The command to run.
	#[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
	command: Vec<OsString>,

	/// The path to write descriptions of processes that outlived the test.
	#[arg(long)]
	leaks: PathBuf,

	/// The test's temporary directory, which identifies its escaped processes on macOS.
	#[arg(long)]
	path: PathBuf,
}

pub fn run(args: &Args) -> Result<u8> {
	// Install the signal handlers before spawning, so that no exit is missed.
	let signals = Signals::new(&[libc::SIGCHLD, libc::SIGHUP, libc::SIGINT, libc::SIGTERM])?;

	// Adopt orphaned descendants, and terminate the test if the runner exits.
	#[cfg(target_os = "linux")]
	adopt()?;

	// Spawn the test in its own process group.
	let mut command = Command::new(&args.command[0]);
	command.args(&args.command[1..]).process_group(0);
	let child = command.spawn()?;
	let pid = libc::pid_t::try_from(child.id())?;

	// Wait for the test to exit or for a signal to stop it.
	let mut status = None;
	let mut signal = None;
	while status.is_none() && signal.is_none() {
		for received in signals.wait(None)? {
			if received != libc::SIGCHLD {
				signal = Some(received);
			}
		}
		status = process::reap()
			.statuses
			.into_iter()
			.find_map(|(reaped, status)| (reaped == pid).then_some(status));
	}

	// Determine the exit code.
	let code = match (status, signal) {
		(Some(status), _) => process::code(status),
		(None, Some(signal)) => u8::try_from(128 + signal).unwrap_or(1),
		(None, None) => unreachable!(),
	};

	// Wait for the remaining processes to exit on their own, then terminate them.
	#[cfg(target_os = "linux")]
	let leaks = if signal.is_some() {
		terminate(&signals)?;
		Vec::new()
	} else {
		settle(&signals)?
	};
	#[cfg(target_os = "macos")]
	let leaks = if signal.is_some() {
		terminate(pid, &args.path)?;
		Vec::new()
	} else {
		settle(pid, &args.path)?
	};

	// Write the descriptions of the processes that outlived the test.
	if !leaks.is_empty() {
		std::fs::write(&args.leaks, leaks.join("\n") + "\n")?;
	}

	Ok(code)
}

#[cfg(target_os = "linux")]
fn adopt() -> Result<()> {
	// SAFETY: prctl with these options has no memory safety preconditions.
	let result = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
	if result != 0 {
		return Err(std::io::Error::last_os_error().into());
	}
	// SAFETY: prctl with these options has no memory safety preconditions.
	let result = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM, 0, 0, 0) };
	if result != 0 {
		return Err(std::io::Error::last_os_error().into());
	}
	Ok(())
}

/// Wait for the descendants to exit on their own. Every orphan is reparented to this process, so the descendants are gone when it has no children. Return descriptions of the processes that had to be terminated.
#[cfg(target_os = "linux")]
fn settle(signals: &Signals) -> Result<Vec<String>> {
	let deadline = Instant::now() + GRACE;
	loop {
		if !process::reap().children {
			return Ok(Vec::new());
		}
		if Instant::now() >= deadline {
			break;
		}
		let received = signals.wait(Some(deadline))?;
		if received.iter().any(|signal| *signal != libc::SIGCHLD) {
			terminate(signals)?;
			return Ok(Vec::new());
		}
	}
	// SAFETY: getpid has no preconditions.
	let root = unsafe { libc::getpid() };
	let leaks = process::describe(&process::descendants(&process::table(), root));
	let survivors = terminate(signals)?;
	let leaks = leaks.into_iter().chain(survivors).collect();
	Ok(leaks)
}

/// Terminate every descendant, first with SIGTERM and then with SIGKILL. Return descriptions of the processes that survived SIGKILL.
#[cfg(target_os = "linux")]
fn terminate(signals: &Signals) -> Result<Vec<String>> {
	// SAFETY: getpid has no preconditions.
	let root = unsafe { libc::getpid() };
	for (signal, duration) in [(libc::SIGTERM, TERMINATE), (libc::SIGKILL, KILL)] {
		let deadline = Instant::now() + duration;
		let mut signaled = std::collections::BTreeSet::new();
		loop {
			if !process::reap().children {
				return Ok(Vec::new());
			}

			// Signal every descendant, including the orphans adopted since the last pass.
			for entry in process::descendants(&process::table(), root) {
				if signaled.insert(entry.pid) {
					process::kill(entry.pid, signal);
				}
			}

			if Instant::now() >= deadline {
				break;
			}
			signals.wait(Some(deadline))?;
		}
	}
	let survivors = process::descendants(&process::table(), root);
	let survivors = process::describe(&survivors)
		.into_iter()
		.map(|description| format!("{description} survived SIGKILL"))
		.collect();
	Ok(survivors)
}

/// Wait for the test's remaining processes to exit on their own. Without a subreaper, they are found by their process group or by the test's temporary directory in their command line. Return descriptions of the processes that had to be terminated.
#[cfg(target_os = "macos")]
fn settle(pid: libc::pid_t, path: &std::path::Path) -> Result<Vec<String>> {
	let entries = remaining(pid, path);
	let pids = entries.iter().map(|entry| entry.pid).collect::<Vec<_>>();
	let running = process::wait(&pids, Instant::now() + GRACE)?;
	if running.is_empty() {
		return Ok(Vec::new());
	}
	let leaks = entries
		.iter()
		.filter(|entry| running.contains(&entry.pid))
		.cloned()
		.collect::<Vec<_>>();
	let leaks = process::describe(&leaks);
	let survivors = terminate(pid, path)?;
	let leaks = leaks.into_iter().chain(survivors).collect();
	Ok(leaks)
}

/// Terminate the test's remaining processes, first with SIGTERM and then with SIGKILL. Return descriptions of the processes that survived SIGKILL.
#[cfg(target_os = "macos")]
fn terminate(pid: libc::pid_t, path: &std::path::Path) -> Result<Vec<String>> {
	for (signal, duration) in [(libc::SIGTERM, TERMINATE), (libc::SIGKILL, KILL)] {
		let entries = remaining(pid, path);
		if entries.is_empty() {
			return Ok(Vec::new());
		}
		process::kill(-pid, signal);
		for entry in &entries {
			process::kill(entry.pid, signal);
		}
		let pids = entries.iter().map(|entry| entry.pid).collect::<Vec<_>>();
		process::wait(&pids, Instant::now() + duration)?;
		process::reap();
	}
	let survivors = remaining(pid, path);
	let survivors = process::describe(&survivors)
		.into_iter()
		.map(|description| format!("{description} survived SIGKILL"))
		.collect();
	Ok(survivors)
}

/// Find the test's processes by its process group or by its temporary directory in their command lines.
#[cfg(target_os = "macos")]
fn remaining(pid: libc::pid_t, path: &std::path::Path) -> Vec<process::Entry> {
	// SAFETY: getpid has no preconditions.
	let root = unsafe { libc::getpid() };
	let path = path.to_string_lossy();
	process::table()
		.into_iter()
		.filter(|entry| entry.pid != root)
		.filter(|entry| entry.group == pid || entry.command.contains(path.as_ref()))
		.collect()
}
