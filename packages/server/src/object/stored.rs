use {crate::Server, tangram_client::prelude::*, tangram_util::serde::is_false};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Output {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 0, default, skip_serializing_if = "is_false")]
	pub subtree: bool,
}

impl Server {
	pub(crate) async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Output>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_stored_postgres(database, id).await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => self.try_get_object_stored_sqlite(database, id).await,
		}
	}

	#[expect(dead_code)]
	pub(crate) async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_stored_batch_postgres(database, ids)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_stored_batch_sqlite(database, ids).await
			},
		}
	}

	pub(crate) async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(Output, tg::object::Metadata)>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_stored_and_metadata_postgres(database, id)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_stored_and_metadata_sqlite(database, id)
					.await
			},
		}
	}

	pub(crate) async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(Output, tg::object::Metadata)>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_stored_and_metadata_batch_postgres(database, ids)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_stored_and_metadata_batch_sqlite(database, ids)
					.await
			},
		}
	}

	#[expect(dead_code)]
	pub(crate) async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::object::Metadata)>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_touch_object_and_get_stored_and_metadata_postgres(database, id, touched_at)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_touch_object_and_get_stored_and_metadata_sqlite(database, id, touched_at)
					.await
			},
		}
	}

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::object::Metadata)>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_touch_object_and_get_stored_and_metadata_batch_postgres(
					database, ids, touched_at,
				)
				.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_touch_object_and_get_stored_and_metadata_batch_sqlite(
					database, ids, touched_at,
				)
				.await
			},
		}
	}
}
