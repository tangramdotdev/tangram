use tangram_client::prelude::*;

pub use tangram_cache::archive::*;

impl super::Cache {
	pub async fn delete_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_archive_queue_item(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_archive_queue_item(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::delete_archive_queue_item(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_archive_queue_item(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_archive_queue_item(arg).await,
		}
	}

	pub async fn get_archive_queue_items(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Item>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.get_archive_queue_items(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_archive_queue_items(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::get_archive_queue_items(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.get_archive_queue_items(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_archive_queue_items(arg).await,
		}
	}

	pub async fn put_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_archive_queue_item(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_archive_queue_item(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::put_archive_queue_item(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_archive_queue_item(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_archive_queue_item(arg).await,
		}
	}

	pub async fn try_get_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Item>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_archive_queue_item(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_archive_queue_item(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::try_get_archive_queue_item(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_archive_queue_item(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_archive_queue_item(arg).await,
		}
	}
}

impl tangram_cache::archive::Cache for super::Cache {
	async fn delete_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_item(arg).await
	}

	async fn get_archive_queue_items(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Item>> {
		self.get_archive_queue_items(arg).await
	}

	async fn put_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_item(arg).await
	}

	async fn try_get_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Item>> {
		self.try_get_archive_queue_item(arg).await
	}
}
