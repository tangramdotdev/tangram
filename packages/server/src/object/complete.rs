use {crate::Server, tangram_client::prelude::*};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn try_get_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_complete_postgres(database, id).await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_complete_sqlite(database, id).await
			},
		}
	}

	#[expect(dead_code)]
	pub(crate) async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_complete_batch_postgres(database, ids)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_complete_batch_sqlite(database, ids)
					.await
			},
		}
	}

	pub(crate) async fn try_get_object_complete_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_complete_and_metadata_postgres(database, id)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_complete_and_metadata_sqlite(database, id)
					.await
			},
		}
	}

	pub(crate) async fn try_get_object_complete_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_complete_and_metadata_batch_postgres(database, ids)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_complete_and_metadata_batch_sqlite(database, ids)
					.await
			},
		}
	}

	#[expect(dead_code)]
	pub(crate) async fn try_touch_object_and_get_complete_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_touch_object_and_get_complete_and_metadata_postgres(
					database, id, touched_at,
				)
				.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_touch_object_and_get_complete_and_metadata_sqlite(database, id, touched_at)
					.await
			},
		}
	}

	pub(crate) async fn try_touch_object_and_get_complete_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_touch_object_and_get_complete_and_metadata_batch_postgres(
					database, ids, touched_at,
				)
				.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_touch_object_and_get_complete_and_metadata_batch_sqlite(
					database, ids, touched_at,
				)
				.await
			},
		}
	}
}
