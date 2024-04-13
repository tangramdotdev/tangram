//! # Configuring the tangram CLI and server
//!
//! Tangram can be configured by a global config file located at $HOME/.config/config.json or by passing the `--config <path>` option to the `tg` command line before any subcommand, for example
//!
//! ```sh
//! # Run the server using a config file.
//! tg --config config.json server run
//! ```
use serde_with::serde_as;
use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
	/// Advanced configuration options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub advanced: Option<Advanced>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub autoenv: Option<Autoenv>,

	/// Configure the server's build options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<Build>,

	/// Configure the server's database options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub database: Option<Database>,

	/// Configure the server's file system monitoring options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_system_monitor: Option<FileSystemMonitor>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub messenger: Option<Messenger>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub oauth: Option<Oauth>,

	/// Configure the server's path. Default = `$HOME/.tangram`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	/// A list of remote servers that this server can push and pull objects/builds from.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remotes: Option<Vec<Remote>>,

	/// Server and CLI tracing options.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub tracing: Option<Tracing>,

	/// The URL of the server, if serving over tcp.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	/// Configurate the virtual file system.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<Vfs>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Advanced {
	/// Configure how errors are displayed in the CLI.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error_trace_options: Option<tg::error::TraceOptions>,

	/// Configure the number of file descriptors available in the client.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_limit: Option<u64>,

	/// Configure the number of file descriptors available in the server.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub file_descriptor_semaphore_size: Option<usize>,

	/// Toggle whether temp directories are preserved or deleted after builds. Default = false.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub preserve_temp_directories: Option<bool>,

	/// Toggle whether tokio-console support is enabled. Default = false.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub tokio_console: bool,

	/// Toggle whether log messages are printed to the server's stderr. Default = false.
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
	/// Toggle whether builds are enabled on this server.
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
pub struct FileSystemMonitor {
	/// Toggle whether the file system monitor is enabled. Default = true.
	pub enable: bool,
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
	Channel,
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
	/// The filter applied to tracing messages.
	#[serde(default, skip_serializing_if = "String::is_empty")]
	pub filter: String,

	/// The display format of tracing messages.
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

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Vfs {
	/// Toggle whether the VFS is enabled. When the VFS is disabled, checkouts will be made onto local disk. Default = true.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,
}
