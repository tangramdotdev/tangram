use tangram_client::prelude::*;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct Discarded {
	pub error: tg::Either<tg::error::Data, tg::error::Id>,
}

#[must_use]
pub fn connected_subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.connected")
}

#[must_use]
pub fn discarded_subject(id: &tg::sandbox::Id) -> String {
	format!("sandboxes.{id}.control.discarded")
}
