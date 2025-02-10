use std::{path::PathBuf, time::Duration};
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
	pub advanced: Advanced,
	pub authentication: Option<Authentication>,
	pub database: Database,
	pub messenger: Messenger,
	pub indexer: Option<Indexer>,
	pub path: PathBuf,
	pub runner: Option<Runner>,
	pub store: Option<Store>,
	pub url: Option<Url>,
	pub version: Option<String>,
	pub vfs: Option<Vfs>,
	pub watchdog: Option<Watchdog>,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub garbage_collection_grace_period: Duration,
	pub process_dequeue_timeout: Duration,
	pub error_trace_options: tg::error::TraceOptions,
	pub file_descriptor_semaphore_size: usize,
	pub preserve_temp_directories: bool,
	pub write_blobs_to_blobs_directory: bool,
	pub write_process_logs_to_database: bool,
	pub write_process_logs_to_stderr: bool,
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
pub enum Database {
	Postgres(PostgresDatabase),
	Sqlite(SqliteDatabase),
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub connections: usize,
	pub url: Url,
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub connections: usize,
	pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Indexer {
	pub batch_size: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug, Default)]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug)]
pub struct NatsMessenger {
	pub url: Url,
}

#[derive(Clone, Debug)]
pub struct Runner {
	pub concurrency: usize,
	pub heartbeat_interval: Duration,
	pub max_depth: u64,
	pub remotes: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum Store {
	Memory,
	Lmdb(LmdbStore),
	S3(S3Store),
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
	pub url: Url,
}

#[derive(Clone, Copy, Debug)]
pub struct Vfs {
	pub cache_ttl: Duration,
	pub cache_size: usize,
	pub database_connections: usize,
}

#[derive(Clone, Debug)]
pub struct Watchdog {
	pub interval: Duration,
	pub limit: usize,
	pub timeout: Duration,
}

impl Config {
	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		let advanced = Advanced::default();
		let authentication = None;
		let database = Database::Sqlite(SqliteDatabase {
			connections: 1,
			path: path.join("database"),
		});
		let indexer = None;
		let messenger = Messenger::default();
		let runner = None;
		let store = None;
		let url = None;
		let version = None;
		let vfs = None;
		let watchdog = None;
		Self {
			advanced,
			authentication,
			database,
			messenger,
			indexer,
			path,
			runner,
			store,
			url,
			version,
			vfs,
			watchdog,
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			garbage_collection_grace_period: Duration::from_secs(0),
			process_dequeue_timeout: Duration::from_secs(3600),
			error_trace_options: tg::error::TraceOptions {
				internal: true,
				reverse: false,
			},
			file_descriptor_semaphore_size: 1_000_000_000,
			preserve_temp_directories: false,
			write_blobs_to_blobs_directory: true,
			write_process_logs_to_database: false,
			write_process_logs_to_stderr: false,
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self {
			concurrency: 1,
			heartbeat_interval: Duration::from_secs(1),
			max_depth: 4096,
			remotes: Vec::new(),
		}
	}
}

impl Default for Watchdog {
	fn default() -> Self {
		Self {
			interval: Duration::from_secs(1),
			limit: 100,
			timeout: Duration::from_secs(60),
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
			batch_size: 128,
			timeout: Duration::from_secs(60),
		}
	}
}

impl Default for NatsMessenger {
	fn default() -> Self {
		let url = "nats://localhost:4222".parse().unwrap();
		Self { url }
	}
}

impl Default for Vfs {
	fn default() -> Self {
		Self {
			cache_ttl: Duration::from_secs(3600),
			cache_size: 4096,
			database_connections: 4,
		}
	}
}
