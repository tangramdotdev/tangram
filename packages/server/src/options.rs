use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

pub struct Options {
	pub address: tg::Address,
	pub advanced: Advanced,
	pub build: Build,
	pub database: Database,
	pub messenger: Messenger,
	pub oauth: Oauth,
	pub path: PathBuf,
	pub remote: Option<Remote>,
	pub url: Option<Url>,
	pub version: String,
	pub vfs: Vfs,
	pub www: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Advanced {
	pub error_trace_options: tangram_error::TraceOptions,
	pub file_descriptor_semaphore_size: usize,
	pub preserve_temp_directories: bool,
	pub write_build_logs_to_stderr: bool,
}

#[derive(Clone, Debug)]
pub struct Build {
	pub enable: bool,
	pub max_concurrency: usize,
}

#[derive(Clone, Debug)]
pub enum Database {
	Sqlite(SqliteDatabase),
	Postgres(PostgresDatabase),
}

#[derive(Clone, Debug)]
pub struct SqliteDatabase {
	pub max_connections: usize,
}

#[derive(Clone, Debug)]
pub struct PostgresDatabase {
	pub url: Url,
	pub max_connections: usize,
}

#[derive(Clone, Debug)]
pub enum Messenger {
	Local,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug)]
pub struct NatsMessenger {
	pub url: Url,
}

#[derive(Clone, Debug, Default)]
pub struct Oauth {
	pub github: Option<OauthClient>,
}

#[derive(Clone, Debug)]
pub struct OauthClient {
	pub client_id: String,
	pub client_secret: String,
	pub auth_url: String,
	pub token_url: String,
}

pub struct StackTrace {
	pub exclude: Vec<glob::Pattern>,
	pub include: Vec<glob::Pattern>,
	pub reverse: bool,
}

pub struct Remote {
	pub build: RemoteBuild,
	pub tg: Box<dyn tg::Handle>,
}

#[derive(Clone, Debug)]
pub struct RemoteBuild {
	pub enable: bool,
	pub hosts: Vec<tg::Triple>,
}

#[derive(Clone, Debug)]
pub struct Vfs {
	pub enable: bool,
}
