use serde_with::serde_as;
use std::path::PathBuf;
use tangram_error::{error, Error};
use url::Url;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub autoenv: Option<Autoenv>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<Build>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub oauth: Option<Oauth>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<Remote>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<Vfs>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Advanced {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error_trace_options: Option<tangram_error::TraceOptions>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_limit: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_semaphore_size: Option<usize>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub tokio_console: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub write_build_logs_to_stderr: Option<bool>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Autoenv {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Build {
	/// Enable builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,

	/// The maximum number of concurrent builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_concurrency: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Database {
	Sqlite(SqliteDatabase),
	Postgres(PostgresDatabase),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SqliteDatabase {
	/// The maximum number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_connections: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PostgresDatabase {
	/// The URL.
	pub url: Url,

	/// The maximum number of connections.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub max_connections: Option<usize>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Messenger {
	#[default]
	Local,
	Nats(NatsMessenger),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct NatsMessenger {
	pub url: Url,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Oauth {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub github: Option<OauthClient>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct OauthClient {
	pub auth_url: String,
	pub client_id: String,
	pub client_secret: String,
	pub redirect_url: String,
	pub token_url: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Remote {
	/// The server's url.
	pub url: Url,

	/// Configure remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<RemoteBuild>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RemoteBuild {
	/// Enable remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,
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
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"compact" => Ok(Self::Compact),
			"json" => Ok(Self::Json),
			"pretty" => Ok(Self::Pretty),
			_ => Err(error!("invalid tracing format")),
		}
	}
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Vfs {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,
}
