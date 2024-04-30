use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Options {
	pub advanced: Advanced,
	pub authentication: Authentication,
	pub build: Option<Build>,
	pub build_monitor: Option<BuildMonitor>,
	pub database: Database,
	pub messenger: Messenger,
	pub path: PathBuf,
	pub remotes: Vec<Remote>,
	pub url: Url,
	pub version: Option<String>,
	pub vfs: bool,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub error_trace_options: tg::error::TraceOptions,
	pub file_descriptor_semaphore_size: usize,
	pub preserve_temp_directories: bool,
	pub write_build_logs_to_file: bool,
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
	pub heartbeat_interval: std::time::Duration,
}

#[derive(Clone, Debug)]
pub struct BuildMonitor {
	pub interval: std::time::Duration,
	pub dequeue_timeout: std::time::Duration,
	pub heartbeat_timeout: std::time::Duration,
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
pub struct Remote {
	pub build: bool,
	pub client: tg::Client,
}
