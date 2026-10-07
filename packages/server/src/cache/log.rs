use {super::Cache, tangram_cache::prelude::*, tangram_client::prelude::*};

pub use tangram_cache::log::*;

impl Cache {
	pub async fn delete_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::log::Cache::delete_log_cache_entry(cache, arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::log::Cache::delete_log_cache_entry(cache, arg).await,
			Self::Memory(cache) => {
				tangram_cache::log::Cache::delete_log_cache_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::log::Cache::delete_log_cache_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::log::Cache::delete_log_cache_entry(cache, arg).await,
		}
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: tangram_cache::log::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::log::cache::Entry>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::log::Cache::get_log_cache_entries(cache, arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::log::Cache::get_log_cache_entries(cache, arg).await,
			Self::Memory(cache) => {
				tangram_cache::log::Cache::get_log_cache_entries(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::log::Cache::get_log_cache_entries(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::log::Cache::get_log_cache_entries(cache, arg).await,
		}
	}

	pub async fn put_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::log::Cache::put_log_cache_entry(cache, arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::log::Cache::put_log_cache_entry(cache, arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::put_log_cache_entry(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::log::Cache::put_log_cache_entry(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::log::Cache::put_log_cache_entry(cache, arg).await,
		}
	}

	pub async fn delete_log(&self, arg: self::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_log(arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::delete_log(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_log(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_log(arg).await,
		}
	}

	pub async fn put_log(&self, arg: self::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_log(arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::put_log(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_log(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_log(arg).await,
		}
	}

	pub async fn put_log_batch(&self, args: Vec<self::put::Arg>) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_log_batch(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_log_batch(args).await,
			Self::Memory(cache) => tangram_cache::log::Cache::put_log_batch(cache, args).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_log_batch(args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_log_batch(args).await,
		}
	}

	pub async fn put_log_end(&self, arg: self::end::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::log::Cache::put_log_end(cache, arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::log::Cache::put_log_end(cache, arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::put_log_end(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::log::Cache::put_log_end(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::log::Cache::put_log_end(cache, arg).await,
		}
	}

	pub async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::log::Cache::try_get_log_end(cache, process).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::log::Cache::try_get_log_end(cache, process).await,
			Self::Memory(cache) => tangram_cache::log::Cache::try_get_log_end(cache, process).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::log::Cache::try_get_log_end(cache, process).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::log::Cache::try_get_log_end(cache, process).await,
		}
	}

	pub async fn try_get_log_length(&self, arg: self::length::Arg) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_log_length(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_log_length(arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::try_get_log_length(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_log_length(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_log_length(arg).await,
		}
	}

	pub async fn try_read_log(
		&self,
		arg: self::read::Arg,
	) -> tg::Result<Vec<self::read::Entry<'static>>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_read_log(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_read_log(arg).await,
			Self::Memory(cache) => tangram_cache::log::Cache::try_read_log(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_read_log(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_read_log(arg).await,
		}
	}
}

impl tangram_cache::log::Cache for Cache {
	async fn delete_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_log_cache_entry(arg).await
	}

	async fn get_log_cache_entries(
		&self,
		arg: tangram_cache::log::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::log::cache::Entry>> {
		self.get_log_cache_entries(arg).await
	}

	async fn put_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_log_cache_entry(arg).await
	}

	async fn delete_log(&self, arg: self::delete::Arg) -> tg::Result<()> {
		self.delete_log(arg).await
	}

	async fn put_log(&self, arg: self::put::Arg) -> tg::Result<()> {
		self.put_log(arg).await
	}

	async fn put_log_batch(&self, args: Vec<self::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args).await
	}

	async fn put_log_end(&self, arg: self::end::Arg) -> tg::Result<()> {
		self.put_log_end(arg).await
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		self.try_get_log_end(process).await
	}

	async fn try_get_log_length(&self, arg: self::length::Arg) -> tg::Result<Option<u64>> {
		self.try_get_log_length(arg).await
	}

	async fn try_read_log(
		&self,
		arg: self::read::Arg,
	) -> tg::Result<Vec<self::read::Entry<'static>>> {
		self.try_read_log(arg).await
	}
}
