use std::{path::PathBuf, time::Duration};
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
	pub advanced: Advanced,
	pub authentication: Option<Authentication>,
	pub database: Database,
	pub messenger: Messenger,
	pub object_indexer: Option<ObjectIndexer>,
	pub path: PathBuf,
	pub process: Option<Process>,
	pub process_heartbeat_monitor: Option<ProcessHeartbeatMonitor>,
	pub process_indexer: Option<ProcessIndexer>,
	pub store: Option<Store>,
	pub url: Option<Url>,
	pub version: Option<String>,
	pub vfs: Option<Vfs>,
}

#[derive(Clone, Debug)]
pub struct Advanced {
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
pub struct Process {
	pub concurrency: usize,
	pub heartbeat_interval: Duration,
	pub max_depth: u64,
	pub remotes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ProcessHeartbeatMonitor {
	pub interval: Duration,
	pub limit: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct ProcessIndexer {}

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
pub struct ObjectIndexer {
	pub batch_size: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug, Default)]
pub enum Store {
	#[default]
	Memory,
	S3(S3Store),
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

impl Config {
	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		let advanced = Advanced::default();
		let authentication = None;
		let database = Database::Sqlite(SqliteDatabase {
			connections: 1,
			path: path.join("database"),
		});
		let messenger = Messenger::default();
		let object_indexer = None;
		let process = None;
		let process_heartbeat_monitor = None;
		let process_indexer = None;
		let store = None;
		let url = None;
		let version = None;
		let vfs = None;
		Self {
			advanced,
			authentication,
			database,
			messenger,
			object_indexer,
			path,
			process,
			process_heartbeat_monitor,
			process_indexer,
			store,
			url,
			version,
			vfs,
		}
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
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

impl Default for Process {
	fn default() -> Self {
		Self {
			concurrency: 1,
			heartbeat_interval: Duration::from_secs(1),
			max_depth: 4096,
			remotes: Vec::new(),
		}
	}
}

impl Default for ProcessHeartbeatMonitor {
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

impl Default for ObjectIndexer {
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
