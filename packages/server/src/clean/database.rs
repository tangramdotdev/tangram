use {
	crate::{Database, Server},
	tangram_client::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn clean_remote_tags(
		&self,
		max_cached_at: i64,
		batch_size: usize,
	) -> tg::Result<u64> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.clean_remote_tags_postgres(database, max_cached_at, batch_size)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				self.clean_remote_tags_sqlite(database, max_cached_at, batch_size)
					.await
			},
		}
	}
}
