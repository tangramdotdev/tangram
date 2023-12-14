use crate::{build, client};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Runtime {
	pub addr: client::Addr,
	pub build: build::Id,
}
