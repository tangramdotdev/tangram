use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn get_indexers(&self) -> tg::Result<Vec<index::indexer::Indexer>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_indexers().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_indexers().await,
		}
	}

	pub async fn put_indexer(&self, arg: index::indexer::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_indexer(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_indexer(arg).await,
		}
	}

	pub async fn try_get_indexer(
		&self,
		arg: index::indexer::get::Arg,
	) -> tg::Result<Option<index::indexer::Indexer>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_indexer(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_indexer(arg).await,
		}
	}

	pub async fn update_indexer(&self, arg: index::indexer::update::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.update_indexer(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.update_indexer(arg).await,
		}
	}
}

impl index::indexer::Index for Index {
	async fn get_indexers(&self) -> tg::Result<Vec<index::indexer::Indexer>> {
		self.get_indexers().await
	}

	async fn put_indexer(&self, arg: index::indexer::put::Arg) -> tg::Result<()> {
		self.put_indexer(arg).await
	}

	async fn try_get_indexer(
		&self,
		arg: index::indexer::get::Arg,
	) -> tg::Result<Option<index::indexer::Indexer>> {
		self.try_get_indexer(arg).await
	}

	async fn update_indexer(&self, arg: index::indexer::update::Arg) -> tg::Result<()> {
		self.update_indexer(arg).await
	}
}
