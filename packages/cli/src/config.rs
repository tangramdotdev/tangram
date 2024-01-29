use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Config {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub autoenv: Option<Autoenv>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build: Option<Build>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<Remote>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub vfs: Option<Vfs>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Autoenv {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub paths: Vec<PathBuf>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Build {
	/// The maximum number of concurrent builds.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub permits: Option<usize>,
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
	pub hosts: Option<Vec<tg::System>>,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Vfs {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub enable: Option<bool>,
}
