use tangram_client::prelude::*;

pub mod put;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Sync {
	#[tangram_serialize(id = 0)]
	pub expires_at: i64,
}

pub trait Index {
	fn try_get_syncs(
		&self,
		ids: &[tg::sync::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<Sync>>>> + Send;
}
