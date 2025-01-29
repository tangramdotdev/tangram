use std::{
	path::{Path, PathBuf},
	time::Duration,
};
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
	pub advanced: Advanced,
	pub authentication: Option<Authentication>,
	pub build: Option<Build>,
	pub build_heartbeat_monitor: Option<BuildHeartbeatMonitor>,
	pub build_indexer: Option<BuildIndexer>,
	pub database: Database,
	pub messenger: Messenger,
	pub object_indexer: Option<ObjectIndexer>,
	pub path: PathBuf,
	pub store: Option<Store>,
	pub url: Url,
	pub version: Option<String>,
	pub vfs: Option<Vfs>,
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug)]
pub struct Advanced {
	pub build_dequeue_timeout: Duration,
	pub error_trace_options: tg::error::TraceOptions,
	pub file_descriptor_semaphore_size: usize,
	pub preserve_temp_directories: bool,
	pub write_blobs_to_blobs_directory: bool,
	pub write_build_logs_to_database: bool,
	pub write_build_logs_to_stderr: bool,
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
pub struct Build {
	pub concurrency: usize,
	pub heartbeat_interval: Duration,
	pub max_depth: u64,
	pub set_path_and_tag: bool,
	pub remotes: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct BuildHeartbeatMonitor {
	pub interval: Duration,
	pub limit: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct BuildIndexer {}

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
		let build = None;
		let build_heartbeat_monitor = None;
		let build_indexer = None;
		let database = Database::Sqlite(SqliteDatabase::with_path(path.join("database")));
		let messenger = Messenger::default();
		let object_indexer = None;
		let store = None;
		let url = Self::default_url_for_path(&path);
		let version = None;
		let vfs = None;
		Self {
			advanced,
			authentication,
			build,
			build_heartbeat_monitor,
			build_indexer,
			database,
			messenger,
			object_indexer,
			path,
			store,
			url,
			version,
			vfs,
		}
	}

	pub fn default_url_for_path(path: impl AsRef<Path>) -> Url {
		let path = path.as_ref().join("socket");
		let path = path.to_str().unwrap();
		let path = urlencoding::encode(path);
		format!("http+unix://{path}").parse().unwrap()
	}
}

impl Default for Advanced {
	fn default() -> Self {
		Self {
			build_dequeue_timeout: Duration::from_secs(3600),
			error_trace_options: tg::error::TraceOptions {
				internal: true,
				reverse: false,
			},
			file_descriptor_semaphore_size: 1_000_000_000,
			preserve_temp_directories: false,
			write_blobs_to_blobs_directory: true,
			write_build_logs_to_database: false,
			write_build_logs_to_stderr: false,
		}
	}
}

impl Default for Build {
	fn default() -> Self {
		let n = std::thread::available_parallelism().unwrap();
		Self {
			concurrency: n.into(),
			heartbeat_interval: Duration::from_secs(1),
			max_depth: 4096,
			set_path_and_tag: false,
			remotes: Vec::new(),
		}
	}
}

impl Default for BuildHeartbeatMonitor {
	fn default() -> Self {
		Self {
			interval: Duration::from_secs(1),
			limit: 100,
			timeout: Duration::from_secs(60),
		}
	}
}

impl SqliteDatabase {
	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		let n = std::thread::available_parallelism().unwrap();
		Self {
			connections: n.into(),
			path,
		}
	}

	#[must_use]
	pub fn with_path_and_connections(path: PathBuf, connections: usize) -> Self {
		Self { connections, path }
	}
}

impl Default for PostgresDatabase {
	fn default() -> Self {
		let n = std::thread::available_parallelism().unwrap();
		Self {
			connections: n.into(),
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
