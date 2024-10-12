use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Options {
	pub advanced: Advanced,
	pub authentication: Authentication,
	pub build: Option<Build>,
	pub build_heartbeat_monitor: Option<BuildHeartbeatMonitor>,
	pub build_indexer: Option<BuildIndexer>,
	pub database: Database,
	pub messenger: Messenger,
	pub object_indexer: Option<ObjectIndexer>,
	pub path: PathBuf,
	pub remotes: BTreeMap<String, Remote>,
	pub url: Url,
	pub version: Option<String>,
	pub vfs: Option<Vfs>,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub build_dequeue_timeout: Option<Duration>,
	pub error_trace_options: tg::error::TraceOptions,
	pub file_descriptor_semaphore_size: usize,
	pub preserve_temp_directories: bool,
	pub write_build_logs_to_database: bool,
	pub write_build_logs_to_stderr: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Authentication {
	pub providers: Option<AuthenticationProviders>,
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
}

#[derive(Clone, Debug)]
pub struct BuildHeartbeatMonitor {
	pub interval: Duration,
	pub limit: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct BuildIndexer {}

#[derive(Clone, Debug)]
pub struct BuildDequeueMonitor {
	pub interval: Duration,
	pub limit: usize,
	pub timeout: Duration,
}

#[derive(Clone, Debug)]
pub enum Database {
	Sqlite(SqliteDatabase),
	Postgres(PostgresDatabase),
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub connections: usize,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub url: Url,
	pub connections: usize,
}

#[derive(Clone, Debug)]
pub enum Messenger {
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

#[derive(Clone, Debug)]
pub struct Remote {
	pub build: bool,
	pub client: tg::Client,
}

#[derive(Clone, Copy, Debug)]
pub struct Vfs {
	pub cache_ttl: Duration,
	pub cache_size: usize,
	pub database_connections: usize,
}
