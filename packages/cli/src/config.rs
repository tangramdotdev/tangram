use serde_with::{serde_as, DurationSecondsWithFrac};
use std::{path::PathBuf, time::Duration};
use tangram_client::{self as tg, util::serde::is_false};
use url::Url;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// Advanced configuration.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	/// Configure authentication.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub authentication: Option<Option<Authentication>>,

	/// Configure builds.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub build: Option<Option<Build>>,

	/// Configure the build heartbeat monitor.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub build_heartbeat_monitor: Option<Option<BuildHeartbeatMonitor>>,

	/// Configure the build indexer.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub build_indexer: Option<Option<BuildIndexer>>,

	/// Configure the database.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	/// Configure the messenger.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	/// Configure the object indexer.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub object_indexer: Option<Option<ObjectIndexer>>,

	/// The path where a client will look for a socket file and where a server will store its data.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	/// Configure tracing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	/// The URL a client will connect to and the server will bind to.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	/// Enable or disable the VFS.
	#[allow(clippy::option_option)]
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "serde_with::rust::double_option"
	)]
	pub vfs: Option<Option<Vfs>>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Advanced {
	/// The duration after which a build that is dequeued but not started may be dequeued again.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub build_dequeue_timeout: Option<Duration>,

	/// Options for rendering error traces.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error_trace_options: Option<tg::error::TraceOptions>,

	/// Set the file descriptor limit for the server on startup.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_limit: Option<usize>,

	/// The maximum number of file descriptors the server will open at a time.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_semaphore_size: Option<usize>,

	/// The path to the preferred autobuild package for `tangram init`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub init_autobuild_reference: Option<tg::Reference>,

	/// Whether to preserve temporary directories.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	/// Whether to enable publishing of data to tokio console.
	#[serde(default, skip_serializing_if = "is_false")]
	pub tokio_console: bool,

	/// Whether to write build logs to the database instead of files.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_database: Option<bool>,

	/// Whether to write build logs to the server's stderr.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_stderr: Option<bool>,

	/// Whether to write blobs to the server's cache directory.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_blobs_to_blobs_directory: Option<bool>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildHeartbeatMonitor {
	/// The duration to pause when there are no builds that need to be canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub interval: Option<Duration>,

	/// The maximum number of builds that will be canceled at a time.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub limit: Option<usize>,

	/// The duration without a heartbeat before a build is canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct BuildIndexer {}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub providers: Option<AuthenticationProviders>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Oauth>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Oauth {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
	/// The maximum number of concurrent builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	/// The interval at which heartbeats will be sent.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<Duration>,

	/// The maximum build depth.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_depth: Option<u64>,

	/// The remotes to build for.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<String>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Postgres(PostgresDatabase),
	Sqlite(SqliteDatabase),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The URL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,

	/// The path.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct NatsMessenger {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectIndexer {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub timeout: Option<Duration>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tracing {
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub filter: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub format: Option<TracingFormat>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	derive_more::Display,
	derive_more::FromStr,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum TracingFormat {
	Compact,
	Hierarchical,
	Json,
	#[default]
	Pretty,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Vfs {
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub cache_ttl: Option<Duration>,
	pub cache_size: Option<usize>,
	pub database_connections: Option<usize>,
}
