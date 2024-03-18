use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

pub struct Options {
	pub address: tg::Address,
	pub build: Build,
	pub database: Database,
	pub file_descriptor_semaphore_size: usize,
	pub messenger: Messenger,
	pub oauth: Oauth,
	pub path: PathBuf,
	pub preserve_temp_directories: bool,
	pub remote: Option<Remote>,
	pub stack_trace: StackTrace,
	pub url: Option<Url>,
	pub version: String,
	pub vfs: Vfs,
	pub write_build_logs_to_stderr: bool,
	pub www: Option<Url>,
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
