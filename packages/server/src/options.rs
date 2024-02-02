use std::path::PathBuf;
use tangram_client as tg;
use url::Url;

pub struct Options {
	pub address: tg::Address,
	pub build: Build,
	pub database: Database,
	pub oauth: Oauth,
	pub path: PathBuf,
	pub remote: Option<Remote>,
	pub url: Option<Url>,
	pub version: String,
	pub vfs: Vfs,
	pub www: Option<Url>,
}

#[derive(Clone, Debug)]
pub struct Build {
	pub enable: bool,
	pub permits: usize,
}

#[derive(Clone, Debug)]
pub struct Database {
	pub url: Option<Url>,
	pub max_connections: usize,
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

pub struct Remote {
	pub build: RemoteBuild,
	pub tg: Box<dyn tg::Handle>,
}

#[derive(Clone, Debug)]
pub struct RemoteBuild {
	pub enable: bool,
	pub hosts: Vec<tg::System>,
}

#[derive(Clone, Debug)]
pub struct Vfs {
	pub enable: bool,
}
