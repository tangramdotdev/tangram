use {super::Cache, tangram_client::prelude::*};

pub use tangram_cache::archive::*;

impl Cache {
	pub async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_archive_queue_entry(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_archive_queue_entry(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::delete_archive_queue_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_archive_queue_entry(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_archive_queue_entry(arg).await,
		}
	}

	pub async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.get_archive_queue_entries(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_archive_queue_entries(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::get_archive_queue_entries(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.get_archive_queue_entries(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_archive_queue_entries(arg).await,
		}
	}

	pub async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_archive_queue_entry(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_archive_queue_entry(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::put_archive_queue_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_archive_queue_entry(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_archive_queue_entry(arg).await,
		}
	}

	pub async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_archive_queue_entry(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_archive_queue_entry(arg).await,
			Self::Memory(cache) => {
				tangram_cache::archive::Cache::try_get_archive_queue_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_archive_queue_entry(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_archive_queue_entry(arg).await,
		}
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_entry(arg).await
	}

	async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		self.get_archive_queue_entries(arg).await
	}

	async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_entry(arg).await
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		self.try_get_archive_queue_entry(arg).await
	}
}
