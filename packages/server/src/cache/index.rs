use {super::Cache, tangram_client::prelude::*};

pub use tangram_cache::index::*;

impl Cache {
	pub async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_index_queue_fragment(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_index_queue_fragment(arg).await,
			Self::Memory(cache) => {
				tangram_cache::index::Cache::delete_index_queue_fragment(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_index_queue_fragment(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_index_queue_fragment(arg).await,
		}
	}

	pub async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.get_index_queue_fragments(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_index_queue_fragments(arg).await,
			Self::Memory(cache) => {
				tangram_cache::index::Cache::get_index_queue_fragments(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.get_index_queue_fragments(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_index_queue_fragments(arg).await,
		}
	}

	pub async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_index_queue_fragment(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_index_queue_fragment(arg).await,
			Self::Memory(cache) => {
				tangram_cache::index::Cache::put_index_queue_fragment(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_index_queue_fragment(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_index_queue_fragment(arg).await,
		}
	}

	pub async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_index_queue_fragment(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_index_queue_fragment(arg).await,
			Self::Memory(cache) => {
				tangram_cache::index::Cache::try_get_index_queue_fragment(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_index_queue_fragment(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_index_queue_fragment(arg).await,
		}
	}
}

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_fragment(arg).await
	}

	async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		self.get_index_queue_fragments(arg).await
	}

	async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_fragment(arg).await
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		self.try_get_index_queue_fragment(arg).await
	}
}
