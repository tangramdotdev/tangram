use crate::prelude::*;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub created_at: i64,

	pub id: tg::token::Id,
}
