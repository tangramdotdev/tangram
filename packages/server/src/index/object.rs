use {super::Index, std::time::Duration, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Vec<tg::object::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_object_children(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_object_children(id).await,
		}
	}

	pub async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_objects(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_objects(ids).await,
		}
	}

	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_objects(ids, touched_at, time_to_touch).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_objects(ids, touched_at, time_to_touch).await,
		}
	}

	pub async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.touch_objects_with_account(ids, account, touched_at, time_to_touch)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.touch_objects_with_account(ids, account, touched_at, time_to_touch)
					.await
			},
		}
	}
}

impl index::object::Index for Index {
	async fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Vec<tg::object::Id>>> {
		self.try_get_object_children(id).await
	}

	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		self.try_get_objects(ids).await
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		self.touch_objects(ids, touched_at, time_to_touch).await
	}

	async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::object::Object>>> {
		self.touch_objects_with_account(ids, account, touched_at, time_to_touch)
			.await
	}
}
