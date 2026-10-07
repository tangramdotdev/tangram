use {super::Cache, tangram_cache::prelude::*, tangram_client::prelude::*};

pub use tangram_cache::object::*;

impl Cache {
	pub async fn contains_object(
		&self,
		arg: tangram_cache::object::contains::Arg,
	) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => tangram_cache::object::Cache::contains_object(cache, arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => tangram_cache::object::Cache::contains_object(cache, arg).await,
			Self::Memory(cache) => tangram_cache::object::Cache::contains_object(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => tangram_cache::object::Cache::contains_object(cache, arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => tangram_cache::object::Cache::contains_object(cache, arg).await,
		}
	}

	pub async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_object_cache_entry(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object_cache_entry(arg).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::delete_object_cache_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_object_cache_entry(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object_cache_entry(arg).await,
		}
	}

	pub async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_object(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object(arg).await,
			Self::Memory(cache) => tangram_cache::object::Cache::delete_object(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_object(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object(arg).await,
		}
	}

	pub async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.delete_object_batch(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.delete_object_batch(args).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::delete_object_batch(cache, args).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.delete_object_batch(args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.delete_object_batch(args).await,
		}
	}

	pub async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.get_object_cache_entries(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.get_object_cache_entries(arg).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::get_object_cache_entries(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.get_object_cache_entries(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.get_object_cache_entries(arg).await,
		}
	}

	pub async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_object_cache_entry(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_cache_entry(arg).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::put_object_cache_entry(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_object_cache_entry(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_cache_entry(arg).await,
		}
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_object_cache_entry_with_object(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_cache_entry_with_object(arg).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::put_object_cache_entry_with_object(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_object_cache_entry_with_object(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_cache_entry_with_object(arg).await,
		}
	}

	pub async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_object(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object(arg).await,
			Self::Memory(cache) => tangram_cache::object::Cache::put_object(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_object(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object(arg).await,
		}
	}

	pub async fn put_object_batch(
		&self,
		args: Vec<tangram_cache::object::put::Arg>,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.put_object_batch(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.put_object_batch(args).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::put_object_batch(cache, args).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.put_object_batch(args).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.put_object_batch(args).await,
		}
	}

	pub async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_object(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object(arg).await,
			Self::Memory(cache) => tangram_cache::object::Cache::try_get_object(cache, arg).await,
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_object(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_object(arg).await,
		}
	}

	pub async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		match self {
			#[cfg(feature = "fjall")]
			Self::Fjall(cache) => cache.try_get_object_batch(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(cache) => cache.try_get_object_batch(arg).await,
			Self::Memory(cache) => {
				tangram_cache::object::Cache::try_get_object_batch(cache, arg).await
			},
			#[cfg(feature = "rocksdb")]
			Self::Rocksdb(cache) => cache.try_get_object_batch(arg).await,
			#[cfg(feature = "scylla")]
			Self::Scylla(cache) => cache.try_get_object_batch(arg).await,
		}
	}
}

impl tangram_cache::object::Cache for Cache {
	async fn contains_object(&self, arg: tangram_cache::object::contains::Arg) -> tg::Result<bool> {
		self.contains_object(arg).await
	}

	async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await
	}

	async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await
	}

	async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn put_object_batch(&self, args: Vec<tangram_cache::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await
	}

	async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		self.try_get_object_batch(arg).await
	}
}
