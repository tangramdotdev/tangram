use {
	crate::Result,
	std::{
		path::{Path, PathBuf},
		process::Stdio,
		sync::LazyLock,
		time::Duration,
	},
	tokio::process::Command,
};

/// The name of the `FoundationDB` container on macOS.
pub const FOUNDATIONDB_CONTAINER: &str = "tangram_test_foundationdb";

/// The time to wait for each database operation.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The statements that delete every row from the PostgreSQL schema, in reverse dependency order.
static POSTGRES_RESET: LazyLock<String> = LazyLock::new(|| {
	let path = crate::repository_path().join("packages/server/src/database/postgres.sql");
	let schema = std::fs::read_to_string(path).unwrap_or_default();
	let regex = regex::Regex::new(r"(?m)^create table ([a-z_]+) \(").unwrap();
	let tables = regex
		.captures_iter(&schema)
		.map(|captures| captures[1].to_owned())
		.collect::<Vec<_>>();
	let mut statements = vec!["begin;".to_owned()];
	for table in tables.iter().rev() {
		statements.push(format!("delete from {table};"));
	}
	statements.push("insert into index_queue_batch (next) values (0);".to_owned());
	statements.push("commit;".to_owned());
	statements.join("\n")
});

/// Get the path of the `FoundationDB` cluster file, writing it if necessary.
pub fn cluster_path() -> Result<PathBuf> {
	if let Some(path) = std::env::var_os("TANGRAM_TEST_FDB_CLUSTER").filter(|path| !path.is_empty())
	{
		return Ok(PathBuf::from(path));
	}
	let path = std::env::temp_dir().join("tangram_test_fdb.cluster");
	std::fs::write(&path, cluster_description())?;
	Ok(path)
}

#[must_use]
pub fn cluster_description() -> &'static str {
	if cfg!(target_os = "linux") {
		"local:local@127.0.0.1:4500"
	} else {
		"docker:docker@127.0.0.1:4500"
	}
}

/// Check that every cloud database is ready.
pub async fn check() -> Result<()> {
	// Check the client programs before attempting the readiness commands.
	let programs: &[&str] = if cfg!(target_os = "linux") {
		&[
			"createdb",
			"dropdb",
			"fdbcli",
			"pg_isready",
			"psql",
			"tangram_scylla_client",
		]
	} else {
		&[
			"createdb",
			"docker",
			"dropdb",
			"pg_isready",
			"psql",
			"tangram_scylla_client",
		]
	};
	let missing = programs
		.iter()
		.filter(|program| which(program).is_none())
		.copied()
		.collect::<Vec<_>>();
	if !missing.is_empty() {
		return Err(format!(
			"the following database clients are missing from PATH: {}\nhelp: install the database programs, then run `nu packages/cli/test.nu --databases` in another terminal",
			missing.join(", ")
		)
		.into());
	}

	// Check each database concurrently so the error names every unavailable service.
	let cluster_path = cluster_path()?;
	let mut postgres = Command::new("pg_isready");
	postgres.args(["--host=127.0.0.1", "--port=5432", "--username=postgres"]);
	let postgres = succeeds(&mut postgres, Duration::from_secs(5));
	let mut foundationdb = foundationdb(&cluster_path, Duration::from_secs(8));
	foundationdb.args(["--exec", "status minimal"]);
	let foundationdb = async {
		foundationdb_listening().await && succeeds(&mut foundationdb, Duration::from_secs(10)).await
	};
	let nats = async {
		let connect = async {
			let mut stream = tokio::net::TcpStream::connect("127.0.0.1:4222")
				.await
				.ok()?;
			let mut buffer = [0u8; 4];
			tokio::io::AsyncReadExt::read_exact(&mut stream, &mut buffer)
				.await
				.ok()?;
			(&buffer == b"INFO").then_some(())
		};
		tokio::time::timeout(Duration::from_secs(2), connect)
			.await
			.ok()
			.flatten()
			.is_some()
	};
	let mut scylla = Command::new("tangram_scylla_client");
	scylla.args([
		"127.0.0.1",
		"9042",
		"-e",
		"select release_version from system.local",
	]);
	let scylla = succeeds(&mut scylla, Duration::from_secs(5));
	let (postgres, foundationdb, nats, scylla) = tokio::join!(postgres, foundationdb, nats, scylla);
	let pool = pool_path().exists();
	let unavailable = [
		("PostgreSQL", postgres),
		("database pool", pool),
		("FoundationDB", foundationdb),
		("NATS", nats),
		("ScyllaDB", scylla),
	]
	.into_iter()
	.filter(|(_, ready)| !ready)
	.map(|(name, _)| name)
	.collect::<Vec<_>>();
	if !unavailable.is_empty() {
		return Err(format!(
			"the following cloud databases are not ready: {}\nhelp: run `nu packages/cli/test.nu --databases` in another terminal",
			unavailable.join(", ")
		)
		.into());
	}

	Ok(())
}

/// Build a command that runs fdbcli against the test cluster.
pub fn foundationdb(cluster_path: &Path, timeout: Duration) -> Command {
	let seconds = timeout.as_secs().to_string();
	if cfg!(target_os = "linux") {
		let mut command = Command::new("timeout");
		command
			.args(["--kill-after=2s", &seconds, "fdbcli", "-C"])
			.arg(cluster_path);
		command
	} else {
		let mut command = Command::new("docker");
		command.args([
			"exec",
			FOUNDATIONDB_CONTAINER,
			"timeout",
			"--kill-after=2s",
			&seconds,
			"fdbcli",
		]);
		command
	}
}

/// Determine whether anything is listening on the `FoundationDB` port, so that callers can skip `fdbcli`, which waits for its own timeout when the cluster is unavailable.
pub async fn foundationdb_listening() -> bool {
	let connect = tokio::net::TcpStream::connect("127.0.0.1:4500");
	matches!(
		tokio::time::timeout(Duration::from_millis(500), connect).await,
		Ok(Ok(_))
	)
}

/// Get the path of the database pool.
#[must_use]
pub fn pool_path() -> PathBuf {
	std::env::temp_dir().join("tangram_test_database_pool")
}

/// Remove the leases whose owner process no longer exists.
pub async fn reclaim() -> Result<()> {
	let pool_path = pool_path();
	let mut count = 0;
	let mut read_dir = tokio::fs::read_dir(&pool_path).await?;
	while let Some(entry) = read_dir.next_entry().await? {
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if !name.starts_with("pool") {
			continue;
		}
		let lease_path = entry.path().join("lease");
		let Ok(lease) = tokio::fs::read_to_string(&lease_path).await else {
			continue;
		};
		let mut lines = lease.lines();
		let owner = lines.next().and_then(|pid| pid.parse::<u32>().ok());
		let start = lines
			.next()
			.map(str::trim)
			.filter(|start| !start.is_empty());
		if let (Some(owner), Some(start)) = (owner, start)
			&& process_start(owner).await.as_deref() == Some(start)
		{
			continue;
		}
		if tokio::fs::remove_file(&lease_path).await.is_ok() {
			count += 1;
		}
	}
	if count > 0 {
		eprintln!("reclaimed {count} stale database pool leases");
	}
	Ok(())
}

/// Reset a database pool instance and release its lease, retrying once.
pub async fn reset(instance: &str, cluster_path: &Path) -> Result<()> {
	let mut error = None;
	for attempt in 0..2 {
		if attempt > 0 {
			tokio::time::sleep(Duration::from_millis(250)).await;
		}
		match reset_once(instance, cluster_path).await {
			Ok(()) => {
				error = None;
				break;
			},
			Err(source) => error = Some(source),
		}
	}
	if let Some(error) = error {
		return Err(format!(
			"failed to reset database pool instance {instance} after 2 attempts: {error}"
		)
		.into());
	}
	let lease_path = pool_path().join(instance).join("lease");
	tokio::fs::remove_file(&lease_path).await.map_err(|error| {
		format!("failed to release the lease for database pool instance {instance}: {error}")
	})?;
	Ok(())
}

/// Run a command with a timeout and capture its output.
pub async fn output(command: &mut Command, timeout: Duration) -> Result<std::process::Output> {
	command
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.kill_on_drop(true);
	let output = tokio::time::timeout(timeout, command.output())
		.await
		.map_err(|_| "the command timed out")??;
	Ok(output)
}

/// Find a program on the PATH.
#[must_use]
pub fn which(program: &str) -> Option<PathBuf> {
	let path = std::env::var_os("PATH")?;
	std::env::split_paths(&path)
		.map(|directory| directory.join(program))
		.find(|path| path.is_file())
}

async fn reset_once(instance: &str, cluster_path: &Path) -> Result<()> {
	let clear = format!("writemode on; clearrange \"{instance}\" \"{instance}\\xff\"");
	let mut foundationdb = foundationdb(cluster_path, TIMEOUT + Duration::from_secs(3));
	foundationdb.args(["--exec", &clear]);
	let database = format!("--dbname=database_{instance}");
	let mut postgres = Command::new("psql");
	postgres.args([
		"--host=127.0.0.1",
		"--username=postgres",
		&database,
		"--set=ON_ERROR_STOP=1",
		"--command",
		&POSTGRES_RESET,
	]);
	let keyspace = format!("cache_{instance}");
	let mut scylla = Command::new("tangram_scylla_client");
	scylla.args([
		"127.0.0.1",
		"9042",
		"-k",
		&keyspace,
		"-e",
		"truncate archive_queue; truncate index_queue; truncate logs; truncate object_cache; truncate objects;",
	]);
	let timeout = TIMEOUT + Duration::from_secs(5);
	let (foundationdb, postgres, scylla) = tokio::join!(
		output(&mut foundationdb, timeout),
		output(&mut postgres, timeout),
		output(&mut scylla, timeout),
	);
	let failures = [
		("foundationdb", foundationdb),
		("postgres", postgres),
		("scylla", scylla),
	]
	.into_iter()
	.filter_map(|(name, output)| match output {
		Ok(output) if output.status.success() => None,
		Ok(output) => {
			let stderr = String::from_utf8_lossy(&output.stderr);
			let stdout = String::from_utf8_lossy(&output.stdout);
			let message = [stderr.trim(), stdout.trim()]
				.into_iter()
				.filter(|message| !message.is_empty())
				.collect::<Vec<_>>()
				.join("\n");
			Some(format!("{name}: {message}"))
		},
		Err(error) => Some(format!("{name}: {error}")),
	})
	.collect::<Vec<_>>();
	if !failures.is_empty() {
		return Err(failures.join("\n").into());
	}
	Ok(())
}

/// Get the start time of a process in the format that the lease records.
async fn process_start(pid: u32) -> Option<String> {
	let output = Command::new("ps")
		.args(["-o", "lstart=", "-p", &pid.to_string()])
		.stderr(Stdio::null())
		.output()
		.await
		.ok()?;
	let start = String::from_utf8_lossy(&output.stdout).trim().to_owned();
	(!start.is_empty()).then_some(start)
}

async fn succeeds(command: &mut Command, timeout: Duration) -> bool {
	output(command, timeout)
		.await
		.is_ok_and(|output| output.status.success())
}
