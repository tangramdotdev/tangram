use either::Either;
use serde_with::serde_as;
use std::{collections::BTreeMap, path::PathBuf};
use tangram_client::{self as tg, util::serde::EitherUntagged};
use url::Url;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Config {
	/// Advanced configuration.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	/// Configure authentication.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authentication: Option<Authentication>,

	/// Configure builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub build: Option<Either<bool, Build>>,

	/// Configure the build heartbeat monitor.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub build_heartbeat_monitor: Option<Either<bool, BuildHeartbeatMonitor>>,

	/// Configure the build indexer.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub build_indexer: Option<Either<bool, BuildIndexer>>,

	/// Configure the database.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	/// Configure the messenger.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	/// Configure the object indexer.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub object_indexer: Option<Either<bool, ObjectIndexer>>,

	/// The path where a client will look for a socket file and where a server will store its data.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	/// Configure the default registry for this server. If this option is set to `true`, then the server will act as the default registry. Otherwise, provide the name of the remote. The default value is "default".
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub registry: Option<Either<bool, String>>,

	/// Configure remotes.
	#[allow(clippy::type_complexity)]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, BTreeMap<_, EitherUntagged<_, _>>>>")]
	pub remotes: Option<Either<bool, BTreeMap<String, Either<bool, Remote>>>>,

	/// Configure tracing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	/// The URL a client will connect to and the server will bind to.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	/// Enable or disable the VFS.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<EitherUntagged<_, _>>")]
	pub vfs: Option<Either<bool, Vfs>>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Advanced {
	/// The duration after which a build that is dequeued but not started may be dequeued again.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build_dequeue_timeout: Option<std::time::Duration>,

	/// Whether to duplicate build logs to the server's stderr.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub duplicate_build_logs_to_stderr: Option<bool>,

	/// Options for rendering error traces.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error_trace_options: Option<tg::error::TraceOptions>,

	/// Set the file descriptor limit for the server on startup.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_limit: Option<u64>,

	/// The maximum number of file descriptors the server will open at a time.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_semaphore_size: Option<usize>,

	/// Whether to preserve temporary directories.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	/// Whether to enable publishing of data to tokio console.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub tokio_console: bool,

	/// Whether to write build logs to the database instead of files.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_database: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct BuildHeartbeatMonitor {
	/// The duration to pause when there are no builds that need to be canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub interval: Option<u64>,

	/// The maximum number of builds that will be canceled at a time.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub limit: Option<u64>,

	/// The duration without a heartbeat before a build is canceled.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<u64>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct BuildIndexer {}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub providers: Option<AuthenticationProviders>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Oauth>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Oauth {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Build {
	/// The maximum number of concurrent builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	/// The heartbeat interval, in seconds. Builds will send a heartbeat at this interval.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub heartbeat_interval: Option<f64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Sqlite(SqliteDatabase),
	Postgres(PostgresDatabase),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqliteDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PostgresDatabase {
	/// The URL.
	pub url: Url,

	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct NatsMessenger {
	pub url: Url,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct ObjectIndexer {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub batch_size: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub timeout: Option<f64>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Remote {
	/// The server's url.
	pub url: Url,

	/// Enable remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
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

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Vfs {
	pub cache_ttl: Option<f64>,
	pub cache_size: Option<u64>,
	pub database_connections: Option<usize>,
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use super::{Config, Remote};

	#[test]
	fn serde() {
		let config = Config {
			remotes: Some(either::Either::Right([
				("default".into(), either::Either::Right(Remote {
					url: "http://foo.bar.baz".parse().unwrap(),
					build: Some(false)
				}))
			].into_iter().collect())),
			..Default::default()
		};
		let json = serde_json::to_string_pretty(&config).unwrap();
		println!("{json}");
	}
}