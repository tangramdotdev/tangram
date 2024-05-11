use either::Either;
use serde_with::serde_as;
use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

use crate::Mode;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
	/// Advanced configuration.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	/// Configure authentication.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub authentication: Option<Authentication>,

	/// Configure builds.
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "either::serde_untagged_optional"
	)]
	pub build: Option<Either<bool, Build>>,

	/// Configure the build monitor.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build_monitor: Option<BuildMonitor>,

	/// Configure the database.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	/// Configure the messenger.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	/// This setting controls whether the CLI runs as a client or a server. When set to `auto`, the CLI will run as a client and start a separate server process if the connection fails or the server's version does not match. If the command is `tg server run`, the mode is set to `server`.
	pub mode: Option<Mode>,

	/// The path where a client will look for a socket file and where a server will store its data.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	/// Configure remotes.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<Remote>>,

	/// Configure tracing.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	/// The URL a client will connect to and the server will bind to.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	/// Enable or disable the VFS.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Advanced {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error_trace_options: Option<tg::error::TraceOptions>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_limit: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_semaphore_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub tokio_console: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_file: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_stderr: Option<bool>,
}

#[derive(Copy, Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct BuildMonitor {
	/// The duration to pause when there are no builds that need to be reenqueued.
	pub dequeue_interval: Option<u64>,

	/// The maximum number of builds that will be reenqueued at a time.
	pub dequeue_limit: Option<u64>,

	/// The duration without being started before a build is reenqueued.
	pub dequeue_timeout: Option<u64>,

	/// The duration to pause when there are no builds that need to be canceled.
	pub heartbeat_interval: Option<u64>,

	/// The maximum number of builds that will be canceled at a time.
	pub heartbeat_limit: Option<u64>,

	/// The duration without a heartbeat before a build is canceled.
	pub heartbeat_timeout: Option<u64>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Authentication {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub providers: Option<AuthenticationProviders>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthenticationProviders {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<Oauth>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Oauth {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Build {
	/// The maximum number of concurrent builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub concurrency: Option<usize>,

	/// The heartbeat interval, in seconds. Builds will send a heartbeat at this interval.
	pub heartbeat_interval: Option<f64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Sqlite(SqliteDatabase),
	Postgres(PostgresDatabase),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqliteDatabase {
	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PostgresDatabase {
	/// The URL.
	pub url: Url,

	/// The number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub connections: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Memory,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NatsMessenger {
	pub url: Url,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Remote {
	/// The server's url.
	pub url: Url,

	/// Enable remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Tracing {
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub filter: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub format: Option<TracingFormat>,
}

#[derive(
	Clone, Copy, Debug, Default, serde_with::SerializeDisplay, serde_with::DeserializeFromStr,
)]
pub enum TracingFormat {
	Compact,
	Json,
	#[default]
	Pretty,
}

impl std::fmt::Display for TracingFormat {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			TracingFormat::Compact => write!(f, "compact"),
			TracingFormat::Json => write!(f, "json"),
			TracingFormat::Pretty => write!(f, "pretty"),
		}
	}
}

impl std::str::FromStr for TracingFormat {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"compact" => Ok(Self::Compact),
			"json" => Ok(Self::Json),
			"pretty" => Ok(Self::Pretty),
			_ => Err(tg::error!("invalid tracing format")),
		}
	}
}
