use serde_with::serde_as;
use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

#[serde_as]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<serde_with::DisplayFromStr>")]
	pub address: Option<tg::Address>,

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
	pub remote: Option<Remote>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<Vfs>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub www: Option<Url>,
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
	pub permits: Option<usize>,
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
	pub token_url: String,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Remote {
	/// The remote URL.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Url>,

	/// Configure remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<RemoteBuild>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct RemoteBuild {
	/// Enable remote builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,

	/// Limit remote builds to targets with the specified hosts.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hosts: Option<Vec<tg::Triple>>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Vfs {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,
}
