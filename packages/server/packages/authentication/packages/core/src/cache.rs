use tangram_client::prelude::*;

pub type Hash = [u8; 32];

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub enum Invalidation {
	Principal(tg::Principal),
	Token(Hash),
}
