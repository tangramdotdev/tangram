use {
	futures::StreamExt as _,
	std::{
		io::Write as _,
		path::{Path, PathBuf},
		process::Stdio,
		sync::LazyLock,
		time::Duration,
	},
	tokio::process::Command,
};

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// The name of the `FoundationDB` container on macOS.
pub const FOUNDATIONDB_CONTAINER: &str = "tangram_test_foundationdb";

/// The time to wait for each database operation.
const TIMEOUT: Duration = Duration::from_secs(30);

/// The statements that delete every row from the PostgreSQL schema, in reverse dependency order.
static POSTGRES_RESET: LazyLock<String> = LazyLock::new(|| {
	let path = repository_path().join("packages/server/packages/database/src/postgres.sql");
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

/// Lease a database pool instance for the run, provisioning a new one if every instance is leased, and return its name.
pub async fn acquire(pool_path: &Path, run: &str) -> Result<String> {
	std::fs::create_dir_all(pool_path)?;
	if let Some(instance) = try_lease(pool_path, run)? {
		return Ok(instance);
	}

	// Serialize provisioning, since ScyllaDB coordinates schema changes globally and a burst of them can overwhelm it.
	let path = pool_path.join("provision.lock");
	let lock = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
		let file = std::fs::File::create(path)?;
		file.lock()?;
		Ok(file)
	})
	.await??;
	if let Some(instance) = try_lease(pool_path, run)? {
		return Ok(instance);
	}
	let instance = create(pool_path, Some(run)).await?;
	drop(lock);

	Ok(instance)
}

/// Determine whether the cloud databases are available to the tests.
#[must_use]
pub fn available() -> bool {
	std::env::var_os("TANGRAM_TEST_CLOUD").is_some_and(|value| !value.is_empty())
}

/// Get the path of the cloud databases' `FoundationDB` cluster file, or print a message and return `None` if the cloud databases are not available, in which case the test should return early.
#[must_use]
pub fn cluster() -> Option<PathBuf> {
	if !available() {
		eprintln!("skipping the test because the cloud databases are not available");
		return None;
	}
	cluster_path().ok()
}

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
			"the following database clients are missing from PATH: {}\nhelp: install the database programs, then run `bun run test:databases` in another terminal",
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
			"the following cloud databases are not ready: {}\nhelp: run `bun run test:databases` in another terminal",
			unavailable.join(", ")
		)
		.into());
	}

	Ok(())
}

/// Build a command that runs fdbcli against the test cluster.
#[must_use]
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

/// A test run's ownership of the database pool leases it acquires, held through an exclusive lock on the run's lock file for the whole run.
pub struct Run {
	id: String,
	_lock: std::fs::File,
}

/// Get the path of the database pool.
#[must_use]
pub fn pool_path() -> PathBuf {
	if let Some(path) =
		std::env::var_os("TANGRAM_TEST_DATABASE_POOL").filter(|path| !path.is_empty())
	{
		return PathBuf::from(path);
	}
	std::env::temp_dir().join("tangram_test_database_pool")
}

/// Provision database pool instances until the pool has the count, creating up to the number of workers at once.
pub async fn provision(pool_path: &Path, count: usize, workers: usize) -> Result<()> {
	std::fs::create_dir_all(pool_path)?;
	let existing = instances(pool_path)?.len();
	let results = futures::stream::iter(existing..count)
		.map(|_| create(pool_path, None))
		.buffer_unordered(workers.max(1))
		.collect::<Vec<_>>()
		.await;
	for result in results {
		result?;
	}
	let actual = instances(pool_path)?.len();
	if actual != count {
		return Err(format!("expected {count} database pool instances, found {actual}").into());
	}

	Ok(())
}

/// Record a database pool instance in the test's temporary directory, so that it is reset after the test.
pub fn record(instance: &str) -> Result<()> {
	let path = std::env::temp_dir().join("instances");
	let mut file = std::fs::File::options()
		.append(true)
		.create(true)
		.open(path)?;
	writeln!(file, "{instance}")?;

	Ok(())
}

/// Remove the leases of runs that have exited, and the lock files of those runs.
pub async fn reclaim() -> Result<()> {
	reclaim_in(&pool_path()).await
}

/// Reset a database pool instance and release its lease, retrying once.
pub async fn reset(instance: &str, cluster_path: &Path, run: &str) -> Result<()> {
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
	release_in(&pool_path(), instance, run).await?;
	Ok(())
}

/// Reset the database pool instances that a test recorded in its temporary directory, and return the errors.
pub async fn reset_recorded(path: &Path, cluster_path: &Path, run: &str) -> Vec<String> {
	let instances = std::fs::read_to_string(path.join("instances")).unwrap_or_default();
	let mut instances = instances
		.lines()
		.filter(|line| !line.is_empty())
		.collect::<Vec<_>>();
	instances.sort_unstable();
	instances.dedup();
	let results = futures::future::join_all(
		instances
			.iter()
			.map(|instance| reset(instance, cluster_path, run)),
	)
	.await;
	results
		.into_iter()
		.filter_map(Result::err)
		.map(|error| error.to_string())
		.collect()
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

impl Run {
	/// Create a run and take its lock. The lock file is locked under a temporary name and then renamed into place, so that another run's reclaim never sees it unlocked.
	pub fn new(pool_path: &Path) -> Result<Self> {
		let runs_path = pool_path.join("runs");
		std::fs::create_dir_all(&runs_path)?;
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)?
			.as_nanos();
		let id = format!("{}-{nanos}", std::process::id());
		let temporary_path = runs_path.join(format!(".{id}.lock"));
		let lock = std::fs::File::create(&temporary_path)?;
		lock.lock()?;
		std::fs::rename(&temporary_path, runs_path.join(format!("{id}.lock")))?;
		let run = Self { id, _lock: lock };
		Ok(run)
	}

	#[must_use]
	pub fn id(&self) -> &str {
		&self.id
	}
}

/// Lease an existing database pool instance if one is free. A lease is created by linking a file that names the run, which fails if the instance is already leased.
fn try_lease(pool_path: &Path, run: &str) -> Result<Option<String>> {
	for instance in instances(pool_path)? {
		let slot_path = pool_path.join(&instance);
		let suffix = rand::random::<u64>();
		let temporary_path = slot_path.join(format!(".lease.{suffix:016x}"));
		std::fs::write(&temporary_path, format!("{run}\n"))?;
		let leased = std::fs::hard_link(&temporary_path, slot_path.join("lease")).is_ok();
		std::fs::remove_file(&temporary_path).ok();
		if leased {
			return Ok(Some(instance));
		}
	}
	Ok(None)
}

/// Get the names of the database pool instances in order.
fn instances(pool_path: &Path) -> Result<Vec<String>> {
	let mut instances = Vec::new();
	for entry in std::fs::read_dir(pool_path)? {
		let entry = entry?;
		let Some(name) = entry.file_name().to_str().map(ToOwned::to_owned) else {
			continue;
		};
		let numbered = name.strip_prefix("pool").is_some_and(|suffix| {
			!suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
		});
		if numbered && entry.file_type()?.is_dir() {
			instances.push(name);
		}
	}
	instances.sort_unstable();
	Ok(instances)
}

/// Create a database pool instance, leased by the run if one is given, and return its name.
async fn create(pool_path: &Path, run: Option<&str>) -> Result<String> {
	// Reserve the next unused name with a temporary directory.
	let (instance, temporary_path) = reserve(pool_path)?;

	// Create the databases, and remove them if any step fails.
	if let Err(error) = create_databases(&instance, &temporary_path, run).await {
		std::fs::remove_dir_all(&temporary_path).ok();
		drop_databases(&instance).await;
		return Err(error);
	}

	// Move the instance into place.
	std::fs::rename(&temporary_path, pool_path.join(&instance))?;

	Ok(instance)
}

fn reserve(pool_path: &Path) -> Result<(String, PathBuf)> {
	let mut index = 0;
	loop {
		let instance = format!("pool{index:04}");
		index += 1;
		let slot_path = pool_path.join(&instance);
		let temporary_path = pool_path.join(format!(".provisioning.{instance}"));
		if slot_path.exists() || std::fs::create_dir(&temporary_path).is_err() {
			continue;
		}
		if slot_path.exists() {
			std::fs::remove_dir(&temporary_path).ok();
			continue;
		}
		return Ok((instance, temporary_path));
	}
}

async fn create_databases(instance: &str, temporary_path: &Path, run: Option<&str>) -> Result<()> {
	// Write the lease.
	if let Some(run) = run {
		std::fs::write(temporary_path.join("lease"), format!("{run}\n"))?;
	}

	// Create and initialize the PostgreSQL database.
	let database = format!("database_{instance}");
	let mut command = Command::new("createdb");
	command.args(["--host=127.0.0.1", "--username=postgres", &database]);
	phase(
		&format!("creating PostgreSQL database {instance}"),
		&mut command,
	)
	.await?;
	let schema_path = repository_path().join("packages/server/packages/database/src/postgres.sql");
	let mut command = Command::new("psql");
	command
		.args([
			"--host=127.0.0.1",
			"--username=postgres",
			&format!("--dbname={database}"),
			"--set=ON_ERROR_STOP=1",
			"--single-transaction",
			"--file",
		])
		.arg(schema_path);
	phase(
		&format!("initializing PostgreSQL database {instance}"),
		&mut command,
	)
	.await?;

	// Create and initialize the ScyllaDB keyspace.
	let keyspace = format!("cache_{instance}");
	let statement = format!(
		"create keyspace \"{keyspace}\" with replication = {{ 'class': 'NetworkTopologyStrategy', 'replication_factor': 1 }};"
	);
	let mut command = Command::new("tangram_scylla_client");
	command.args(["127.0.0.1", "9042", "-e", &statement]);
	phase(
		&format!("creating ScyllaDB keyspace {instance}"),
		&mut command,
	)
	.await?;
	let schema_path = repository_path().join("packages/cache/packages/scylla/schema.cql");
	let mut command = Command::new("tangram_scylla_client");
	command
		.args(["127.0.0.1", "9042", "-k", &keyspace, "-f"])
		.arg(schema_path);
	phase(
		&format!("initializing ScyllaDB keyspace {instance}"),
		&mut command,
	)
	.await?;

	Ok(())
}

async fn drop_databases(instance: &str) {
	let statement = format!("drop keyspace if exists \"cache_{instance}\";");
	let mut scylla = Command::new("tangram_scylla_client");
	scylla.args(["127.0.0.1", "9042", "-e", &statement]);
	let database = format!("database_{instance}");
	let mut postgres = Command::new("dropdb");
	postgres.args([
		"--host=127.0.0.1",
		"--username=postgres",
		"--if-exists",
		"--force",
		&database,
	]);
	let timeout = Duration::from_secs(10);
	let (_scylla, _postgres) =
		tokio::join!(output(&mut scylla, timeout), output(&mut postgres, timeout));
}

/// Run a step of creating a database pool instance, and name the step in its error.
async fn phase(name: &str, command: &mut Command) -> Result<()> {
	let output = output(command, TIMEOUT)
		.await
		.map_err(|error| format!("{name} failed: {error}"))?;
	if !output.status.success() {
		let stderr = String::from_utf8_lossy(&output.stderr);
		return Err(format!("{name} failed: {}", stderr.trim()).into());
	}
	Ok(())
}

fn repository_path() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../..")
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

async fn reclaim_in(pool_path: &Path) -> Result<()> {
	// Remove the leases whose runs are not alive.
	let mut count = 0;
	let mut read_dir = tokio::fs::read_dir(pool_path).await?;
	while let Some(entry) = read_dir.next_entry().await? {
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if !name.starts_with("pool") {
			continue;
		}
		let lease_path = entry.path().join("lease");
		let Ok(owner) = tokio::fs::read_to_string(&lease_path).await else {
			continue;
		};
		if run_alive(pool_path, owner.trim()) {
			continue;
		}
		if tokio::fs::remove_file(&lease_path).await.is_ok() {
			count += 1;
		}
	}
	if count > 0 {
		eprintln!("reclaimed {count} stale database pool leases");
	}

	// Remove the lock files of runs that have exited.
	let runs_path = pool_path.join("runs");
	if let Ok(mut read_dir) = tokio::fs::read_dir(&runs_path).await {
		while let Some(entry) = read_dir.next_entry().await? {
			let path = entry.path();
			if path.extension().is_none_or(|extension| extension != "lock") {
				continue;
			}
			let Ok(file) = std::fs::File::open(&path) else {
				continue;
			};
			if file.try_lock().is_ok() {
				tokio::fs::remove_file(&path).await.ok();
			}
		}
	}

	Ok(())
}

/// Release a lease only if it still names the run, so that a run never deletes a lease that another run acquired after a reclaim.
async fn release_in(pool_path: &Path, instance: &str, run: &str) -> Result<()> {
	let lease_path = pool_path.join(instance).join("lease");
	let owner = tokio::fs::read_to_string(&lease_path).await.map_err(|error| {
		format!("failed to read the lease for database pool instance {instance}, which may have been reclaimed by another run: {error}")
	})?;
	if owner.trim() != run {
		return Err(format!(
			"the lease for database pool instance {instance} is owned by run {}, not this run",
			owner.trim()
		)
		.into());
	}
	tokio::fs::remove_file(&lease_path).await.map_err(|error| {
		format!("failed to release the lease for database pool instance {instance}: {error}")
	})?;
	Ok(())
}

/// Determine whether a run is alive, which is the case when its lock file exists and its lock is held.
fn run_alive(pool_path: &Path, run: &str) -> bool {
	if run.is_empty() || run.contains('/') {
		return false;
	}
	let path = pool_path.join("runs").join(format!("{run}.lock"));
	let Ok(file) = std::fs::File::open(&path) else {
		return false;
	};
	matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
}

async fn succeeds(command: &mut Command, timeout: Duration) -> bool {
	output(command, timeout)
		.await
		.is_ok_and(|output| output.status.success())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pool() -> PathBuf {
		let nanos = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		let pool_path =
			std::env::temp_dir().join(format!("tangram_test_pool_{}_{nanos}", std::process::id()));
		std::fs::create_dir_all(pool_path.join("pool0000")).unwrap();
		pool_path
	}

	#[tokio::test]
	async fn reclaim_keeps_a_lease_whose_run_is_alive_after_its_test_process_exits() {
		let pool_path = pool();
		let run = Run::new(&pool_path).unwrap();
		let lease_path = pool_path.join("pool0000/lease");
		tokio::fs::write(&lease_path, format!("{}\n", run.id()))
			.await
			.unwrap();

		// The test process that acquired the lease has exited, but the run is still cleaning up.
		reclaim_in(&pool_path).await.unwrap();
		let kept = lease_path.exists();
		tokio::fs::remove_dir_all(&pool_path).await.unwrap();
		assert!(kept, "the lease of a live run must not be reclaimed");
	}

	#[tokio::test]
	async fn reclaim_removes_the_lease_and_lock_file_of_an_exited_run() {
		let pool_path = pool();
		let run = Run::new(&pool_path).unwrap();
		let id = run.id().to_owned();
		let lease_path = pool_path.join("pool0000/lease");
		tokio::fs::write(&lease_path, format!("{id}\n"))
			.await
			.unwrap();
		drop(run);

		reclaim_in(&pool_path).await.unwrap();
		let lease = lease_path.exists();
		let lock = pool_path.join("runs").join(format!("{id}.lock")).exists();
		tokio::fs::remove_dir_all(&pool_path).await.unwrap();
		assert!(!lease, "the lease of an exited run must be reclaimed");
		assert!(!lock, "the lock file of an exited run must be removed");
	}

	#[tokio::test]
	async fn release_deletes_only_a_lease_that_names_the_run() {
		let pool_path = pool();
		let run = Run::new(&pool_path).unwrap();
		let other = Run::new(&pool_path).unwrap();
		let lease_path = pool_path.join("pool0000/lease");
		tokio::fs::write(&lease_path, format!("{}\n", other.id()))
			.await
			.unwrap();

		let foreign = release_in(&pool_path, "pool0000", run.id()).await;
		let kept = lease_path.exists();
		let own = release_in(&pool_path, "pool0000", other.id()).await;
		let released = !lease_path.exists();
		let missing = release_in(&pool_path, "pool0000", other.id()).await;
		tokio::fs::remove_dir_all(&pool_path).await.unwrap();
		assert!(
			foreign.is_err() && kept,
			"a run must not release another run's lease"
		);
		assert!(own.is_ok() && released, "a run must release its own lease");
		assert!(missing.is_err(), "releasing a reclaimed lease must fail");
	}
}
