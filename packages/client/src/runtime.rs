use crate::{build, Addr};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Runtime {
	pub addr: Addr,
	pub build: build::Id,
}
