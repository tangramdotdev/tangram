use {
	std::{path::PathBuf, time::Duration},
	tangram_uri::Uri,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub advanced: Advanced,
	pub authentication: Option<Authentication>,
	pub authorization: bool,
	pub cleaner: Option<Cleaner>,
	pub database: Database,
	pub directory: PathBuf,
	pub http: Option<Http>,
	pub index: Index,
	pub indexer: Option<Indexer>,
	pub messenger: Messenger,
	pub remotes: Option<Vec<Remote>>,
	pub runner: Option<Runner>,
	pub store: Store,
	pub version: Option<String>,
	pub vfs: Option<Vfs>,
	pub watchdog: Option<Watchdog>,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub disable_version_check: bool,
	pub internal_error_locations: bool,
	pub preserve_temp_directories: bool,
	pub process_dequeue_timeout: Duration,
	pub shared_directory: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Authentication {
	pub providers: AuthenticationProviders,
}

#[derive(Clone, Debug, Default)]
pub struct AuthenticationProviders {
	pub github: Option<Oauth>,
}

#[derive(Clone, Debug)]
pub struct Oauth {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug)]
pub struct Cleaner {
	pub batch_size: usize,
	pub ttl: Duration,
}

#[derive(Clone, Debug)]
pub enum Database {
	Postgres(PostgresDatabase),
	Sqlite(SqliteDatabase),
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub connections: usize,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub connections: usize,
	pub path: PathBuf,
}

#[derive(Clone, Debug, Default)]
pub struct Http {
	pub url: Option<Uri>,
}

#[derive(Clone, Debug)]
pub enum Index {
	Postgres(PostgresIndex),
	Sqlite(SqliteIndex),
}

#[derive(Clone, Debug)]
pub struct PostgresIndex {
	pub connections: usize,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct SqliteIndex {
	pub connections: usize,
	pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Indexer {
	pub insert_batch_size: usize,
	pub message_batch_size: usize,
	pub message_batch_timeout: Duration,
	pub queue_batch_size: usize,
}

#[derive(Clone, Debug, Default)]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug)]
pub struct NatsMessenger {
	pub credentials: Option<PathBuf>,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct Remote {
	pub name: String,
	pub url: Uri,
	pub token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Runner {
	pub concurrency: usize,
	pub heartbeat_interval: Duration,
	pub remotes: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum Store {
	Fdb(FdbStore),
	Lmdb(LmdbStore),
	Memory,
	S3(S3Store),
	Scylla(ScyllaStore),
}

#[derive(Clone, Debug)]
pub struct FdbStore {
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct LmdbStore {
	pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct S3Store {
	pub access_key: Option<String>,
	pub bucket: String,
	pub region: Option<String>,
	pub secret_key: Option<String>,
	pub url: Uri,
}

#[derive(Clone, Debug)]
pub struct ScyllaStore {
	pub addr: String,
	pub keyspace: String,
	pub password: Option<String>,
	pub username: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct Vfs {
	pub cache_size: usize,
	pub cache_ttl: Duration,
	pub database_connections: usize,
}

#[derive(Clone, Debug)]
pub struct Watchdog {
	pub batch_size: usize,
	pub interval: Duration,
	pub max_depth: usize,
	pub ttl: Duration,
}

impl Config {
	#[must_use]
	pub fn with_directory(directory: PathBuf) -> Self {
		let advanced = Advanced::default();
		let authentication = None;
		let authorization = false;
		let cleaner = None;
		let database = Database::Sqlite(SqliteDatabase {
			connections: 1,
			path: directory.join("database"),
		});
		let index = Index::Sqlite(SqliteIndex {
			connections: 1,
			path: directory.join("index"),
		});
		let indexer = Some(Indexer::default());
		let messenger = Messenger::default();
		let remotes = None;
		let runner = Some(Runner::default());
		let store = Store::Lmdb(LmdbStore {
			path: directory.join("store"),
		});
		let http = Some(Http::default());
		let version = None;
		let vfs = None;
		let watchdog = Some(Watchdog::default());
		Self {
			advanced,
			authentication,
			authorization,
			cleaner,
			database,
			directory,
			http,
			index,
			indexer,
			messenger,
			remotes,
			runner,
			store,
			version,
			vfs,
			watchdog,
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			disable_version_check: false,
			internal_error_locations: false,
			process_dequeue_timeout: Duration::from_secs(3600),
			preserve_temp_directories: false,
			shared_directory: true,
		}
	}
}

impl Default for Cleaner {
	fn default() -> Self {
		Self {
			batch_size: 1024,
			ttl: Duration::from_secs(86400),
		}
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		Self {
			connections: 1,
			url: "postgres://localhost:5432".parse().unwrap(),
		}
	}
}

impl Default for Indexer {
	fn default() -> Self {
		Self {
			insert_batch_size: 1024,
			message_batch_size: 1024,
			message_batch_timeout: Duration::from_millis(100),
			queue_batch_size: 1024,
		}
	}
}

impl Default for NatsMessenger {
	fn default() -> Self {
		let url = "nats://localhost:4222".parse().unwrap();
		Self {
			credentials: None,
			url,
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			concurrency: 1,
			heartbeat_interval: Duration::from_secs(1),
			remotes: Vec::new(),
		}
	}
}

impl Default for Vfs {
	fn default() -> Self {
		Self {
			cache_size: 4096,
			cache_ttl: Duration::from_secs(3600),
			database_connections: 4,
		}
	}
}

impl Default for Watchdog {
	fn default() -> Self {
		Self {
			batch_size: 100,
			interval: Duration::from_secs(1),
			max_depth: 1024,
			ttl: Duration::from_secs(60),
		}
	}
}
