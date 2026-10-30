use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_syncs(
		&self,
		ids: &[tg::sync::Id],
	) -> tg::Result<Vec<Option<index::sync::Sync>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_syncs(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_syncs(ids).await,
		}
	}
}

impl index::sync::Index for Index {
	async fn try_get_syncs(
		&self,
		ids: &[tg::sync::Id],
	) -> tg::Result<Vec<Option<index::sync::Sync>>> {
		self.try_get_syncs(ids).await
	}
}
