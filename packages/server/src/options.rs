use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug)]
pub struct Options {
	pub advanced: Advanced,
	pub build: Build,
	pub database: Database,
	pub messenger: Messenger,
	pub oauth: Oauth,
	pub path: PathBuf,
	pub remotes: Vec<Remote>,
	pub url: Url,
	pub version: String,
	pub vfs: Vfs,
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
	Channel,
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
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug)]
pub struct StackTrace {
	pub exclude: Vec<glob::Pattern>,
	pub include: Vec<glob::Pattern>,
	pub reverse: bool,
}

#[derive(Clone, Debug)]
pub struct Remote {
	pub build: RemoteBuild,
	pub client: tg::Client,
}

#[derive(Clone, Debug)]
pub struct RemoteBuild {
	pub enable: bool,
}

#[derive(Clone, Debug)]
pub struct Vfs {
	pub enable: bool,
}
