use crate::Server;
use tangram_client::{self as tg, util::serde::is_false};

#[cfg(feature = "postgres")]
mod postgres;
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
	pub children: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 1, default, skip_serializing_if = "is_false")]
	pub commands: bool,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(id = 2, default, skip_serializing_if = "is_false")]
	pub outputs: bool,
}

impl Server {
	pub(crate) async fn try_get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_postgres(database, id).await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_sqlite(database, id).await
			},
		}
	}

	pub(crate) async fn try_get_process_complete_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_batch_postgres(database, ids)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_batch_sqlite(database, ids)
					.await
			},
		}
	}

	#[allow(dead_code)]
	pub(crate) async fn try_get_process_complete_and_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<(super::complete::Output, tg::process::Metadata)>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_and_metadata_postgres(database, id)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_and_metadata_sqlite(database, id)
					.await
			},
		}
	}

	#[allow(dead_code)]
	pub(crate) async fn try_get_process_complete_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(super::complete::Output, tg::process::Metadata)>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_and_metadata_batch_postgres(database, ids)
					.await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_and_metadata_batch_sqlite(database, ids)
					.await
			},
		}
	}
}
