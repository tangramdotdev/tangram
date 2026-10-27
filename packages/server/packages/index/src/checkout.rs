use {super::Index, std::time::Duration, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<index::checkout::Checkout>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_checkouts(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_checkouts(ids).await,
		}
	}

	pub async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::checkout::Checkout>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_checkouts(ids, touched_at, time_to_touch).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_checkouts(ids, touched_at, time_to_touch).await,
		}
	}
}

impl index::checkout::Index for Index {
	async fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<index::checkout::Checkout>>> {
		self.try_get_checkouts(ids).await
	}

	async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::checkout::Checkout>>> {
		self.touch_checkouts(ids, touched_at, time_to_touch).await
	}
}
