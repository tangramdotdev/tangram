use {
	crate::{Result, database, semaphore, temp, test, vfs},
	std::{
		ffi::CString, os::unix::ffi::OsStrExt as _, path::PathBuf, process::ExitCode,
		time::Duration,
	},
	tokio::process::Command,
};

/// Clean up the test resources that interrupted runs left behind in the databases, processes, temporary directories, and semaphores.
#[derive(clap::Args)]
pub struct Args {
	/// Stop the release build of the `FSKit` provider instead of the development build.
	#[arg(long)]
	release: bool,
}

pub async fn clean(args: Args) -> Result<ExitCode> {
	// Find the temporary directories and the LMDB SysV semaphore keys of their lock files.
	let mut temp_paths = Vec::new();
	for parent in [std::env::temp_dir(), test::fskit_temp_root()] {
		let Ok(read_dir) = std::fs::read_dir(&parent) else {
			continue;
		};
		for entry in read_dir.flatten() {
			let is_directory = entry.file_type().is_ok_and(|file_type| file_type.is_dir());
			if is_directory
				&& entry
					.file_name()
					.to_string_lossy()
					.contains("tangram_test_")
			{
				temp_paths.push(entry.path());
			}
		}
	}
	let pool_path = database::pool_path();
	temp_paths.retain(|path| *path != pool_path);
	let keys = sysv_keys(&temp_paths);

	// Clean the databases.
	clean_postgres().await;
	clean_scylla().await;
	clean_foundationdb().await;

	// Recreate the database pool.
	if pool_path.exists() {
		tokio::fs::remove_dir_all(&pool_path).await?;
	}
	tokio::fs::create_dir_all(&pool_path).await?;

	// Stop the Tangram processes.
	let count = stop_tangram_processes().await?;
	eprintln!("cleaned tangram processes: {count}");

	// Unmount the VFS, unlink the semaphores, and remove the temporary directories.
	for path in &temp_paths {
		vfs::unmount(path, true).await?;
	}
	#[cfg(target_os = "macos")]
	stop_fskit_provider(args.release).await;
	#[cfg(target_os = "linux")]
	let _ = args.release;
	for path in temp_paths {
		let result = tokio::task::spawn_blocking(move || {
			semaphore::cleanup(&path)?;
			temp::remove(&path)?;
			Ok::<_, crate::Error>(path)
		})
		.await?;
		match result {
			Ok(path) => eprintln!("removed {}", path.display()),
			Err(error) => eprintln!("{error}"),
		}
	}

	// Remove the LMDB SysV semaphores.
	let count = remove_sysv_semaphores(&keys).await;
	eprintln!("cleaned lmdb sysv semaphores: {count}");

	Ok(ExitCode::SUCCESS)
}

async fn clean_foundationdb() {
	let Ok(cluster_path) = database::cluster_path() else {
		eprintln!("skipping FoundationDB cleanup because the cluster file could not be written");
		return;
	};
	let mut command = database::foundationdb(&cluster_path, Duration::from_secs(5));
	command.args(["--exec", "writemode on; clearrange \"\" \\xff"]);
	match database::output(&mut command, Duration::from_secs(10)).await {
		Ok(output) if output.status.success() => eprintln!("cleared FoundationDB test data"),
		_ => eprintln!("skipping FoundationDB cleanup because it is not ready"),
	}
}

async fn clean_postgres() {
	let mut command = Command::new("psql");
	command.args([
		"--host=127.0.0.1",
		"--username=postgres",
		"--dbname=postgres",
		"--tuples-only",
		"--no-align",
		"--command",
		"select datname from pg_database",
	]);
	let output = database::output(&mut command, Duration::from_secs(5)).await;
	let Some(output) = output
		.as_ref()
		.ok()
		.filter(|output| output.status.success())
	else {
		eprintln!("skipping PostgreSQL cleanup because it is not ready");
		return;
	};
	let stdout = String::from_utf8_lossy(&output.stdout);
	for name in stdout
		.lines()
		.map(str::trim)
		.filter(|name| name.starts_with("database_"))
	{
		eprintln!("dropping PostgreSQL database {name}");
		let mut command = Command::new("dropdb");
		command.args([
			"--host=127.0.0.1",
			"--username=postgres",
			"--if-exists",
			"--force",
			name,
		]);
		database::output(&mut command, Duration::from_secs(30))
			.await
			.ok();
	}
}

async fn clean_scylla() {
	let mut command = Command::new("tangram_scylla_client");
	command.args([
		"127.0.0.1",
		"9042",
		"-e",
		"SELECT JSON keyspace_name FROM system_schema.keyspaces",
	]);
	let output = database::output(&mut command, Duration::from_secs(5)).await;
	let Some(output) = output
		.as_ref()
		.ok()
		.filter(|output| output.status.success())
	else {
		eprintln!("skipping ScyllaDB cleanup because it is not ready");
		return;
	};
	let stdout = String::from_utf8_lossy(&output.stdout);
	let keyspaces = stdout
		.lines()
		.filter_map(|line| serde_json::from_str::<serde_json::Value>(line.trim()).ok())
		.filter_map(|value| value.get("keyspace_name")?.as_str().map(ToOwned::to_owned))
		.filter(|name| name.starts_with("cache_"));
	for keyspace in keyspaces {
		eprintln!("dropping scylla keyspace {keyspace}");
		let mut command = Command::new("tangram_scylla_client");
		command.args([
			"127.0.0.1",
			"9042",
			"-e",
			&format!("drop keyspace \"{keyspace}\";"),
		]);
		database::output(&mut command, Duration::from_secs(60))
			.await
			.ok();
	}
}

/// Remove the `SysV` semaphores with the given keys that belong to the current user.
async fn remove_sysv_semaphores(keys: &[String]) -> usize {
	if keys.is_empty() {
		return 0;
	}
	let output = database::output(Command::new("ipcs").arg("-s"), Duration::from_secs(5)).await;
	let Ok(output) = output else {
		return 0;
	};
	let user = std::env::var("USER").unwrap_or_default();
	let stdout = String::from_utf8_lossy(&output.stdout);
	let ids = stdout
		.lines()
		.skip(3)
		.filter_map(|line| {
			let columns = line.split_whitespace().collect::<Vec<_>>();
			let matches = columns.len() >= 5
				&& keys.iter().any(|key| key == columns[2])
				&& (user.is_empty() || columns[4] == user);
			matches.then(|| columns[1].to_owned())
		})
		.collect::<Vec<_>>();
	let mut count = 0;
	for id in ids {
		let output = database::output(
			Command::new("ipcrm").args(["-s", &id]),
			Duration::from_secs(5),
		)
		.await;
		if output.is_ok_and(|output| output.status.success()) {
			count += 1;
		}
	}
	count
}

/// Stop every Tangram process. Return the number stopped.
async fn stop_tangram_processes() -> Result<usize> {
	let pids = tangram_processes().await;
	if pids.is_empty() {
		return Ok(0);
	}
	for signal in [libc::SIGTERM, libc::SIGKILL] {
		for pid in tangram_processes().await {
			// SAFETY: kill has no memory safety preconditions.
			unsafe {
				libc::kill(-pid, signal);
				libc::kill(pid, signal);
			}
		}
		for _ in 0..100 {
			if tangram_processes().await.is_empty() {
				return Ok(pids.len());
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}
	Err("test cleanup could not stop all Tangram processes".into())
}

/// Find the processes whose executable is the tangram binary, including through the tg link.
async fn tangram_processes() -> Vec<libc::pid_t> {
	let output = database::output(
		Command::new("ps").args(["-axo", "pid=,command="]),
		Duration::from_secs(5),
	)
	.await;
	let Ok(output) = output else {
		return Vec::new();
	};
	// SAFETY: getpid has no preconditions.
	let current = unsafe { libc::getpid() };
	let stdout = String::from_utf8_lossy(&output.stdout);
	stdout
		.lines()
		.filter_map(|line| {
			let (pid, command) = line.trim().split_once(' ')?;
			let pid = pid.parse::<libc::pid_t>().ok()?;
			let executable = PathBuf::from(command.trim().split(' ').next()?);
			let name = executable.file_name()?.to_str()?;
			if !matches!(name, "tangram" | "tg") || pid == current {
				return None;
			}
			let resolved = executable_path(pid)?;
			(resolved.file_name()? == "tangram").then_some(pid)
		})
		.collect()
}

/// Get the path of a process's executable, which does not depend on how its first argument was written.
#[cfg(target_os = "linux")]
fn executable_path(pid: libc::pid_t) -> Option<PathBuf> {
	std::fs::read_link(format!("/proc/{pid}/exe")).ok()
}

/// Get the path of a process's executable, which does not depend on how its first argument was written.
#[cfg(target_os = "macos")]
fn executable_path(pid: libc::pid_t) -> Option<PathBuf> {
	let mut buffer = vec![0u8; 4096];
	let size = u32::try_from(buffer.len()).ok()?;
	// SAFETY: The buffer is valid for writes of its length.
	let length = unsafe { libc::proc_pidpath(pid, buffer.as_mut_ptr().cast(), size) };
	let length = usize::try_from(length).ok().filter(|length| *length > 0)?;
	buffer.truncate(length);
	let path = <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(buffer);
	Some(PathBuf::from(path))
}

/// Compute the `SysV` IPC keys of the LMDB lock files under the paths.
fn sysv_keys(paths: &[PathBuf]) -> Vec<String> {
	let mut keys = Vec::new();
	for path in paths {
		temp::walk(path, &mut |path| {
			let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
				return;
			};
			if !matches!(name, "index-lock" | "logs-lock" | "objects-lock") {
				return;
			}
			let Ok(path) = CString::new(path.as_os_str().as_bytes()) else {
				return;
			};
			// SAFETY: The path is a valid C string.
			let key = unsafe { libc::ftok(path.as_ptr(), libc::c_int::from(b'M')) };
			if key != -1 {
				keys.push(format!("0x{key:08x}"));
			}
		});
	}
	keys.sort();
	keys.dedup();
	keys
}

/// Stop the FSKit provider so that it does not keep stale mounts.
#[cfg(target_os = "macos")]
async fn stop_fskit_provider(release: bool) {
	let name = if release { "Tangram" } else { "Tangram Dev" };
	let executable = PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(format!(
		"Applications/{name}.app/Contents/Extensions/TangramFSKit.appex/Contents/MacOS/TangramFSKit"
	));
	let executable = executable.to_string_lossy().into_owned();
	let find = || async {
		let output = database::output(
			Command::new("ps").args(["-axo", "pid=,command="]),
			Duration::from_secs(5),
		)
		.await;
		let Ok(output) = output else {
			return Vec::new();
		};
		String::from_utf8_lossy(&output.stdout)
			.lines()
			.filter_map(|line| {
				let (pid, command) = line.trim().split_once(' ')?;
				command
					.trim()
					.starts_with(&executable)
					.then(|| pid.parse::<libc::pid_t>().ok())
					.flatten()
			})
			.collect::<Vec<_>>()
	};
	for signal in [libc::SIGTERM, libc::SIGKILL] {
		for pid in find().await {
			// SAFETY: kill has no memory safety preconditions.
			unsafe {
				libc::kill(pid, signal);
			}
		}
		for _ in 0..100 {
			if find().await.is_empty() {
				return;
			}
			tokio::time::sleep(Duration::from_millis(50)).await;
		}
	}
}
