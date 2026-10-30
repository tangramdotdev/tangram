use {tangram_client::prelude::*, tangram_index as index};

mod checkout;
mod group;
mod indexer;
mod object;
mod organization;
mod permission;
mod process;
mod sandbox;
mod sync;
mod tag;
mod usage;
mod user;

#[derive(derive_more::IsVariant, derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Index {
	#[cfg(feature = "foundationdb")]
	Fdb(tangram_index_fdb::Index),
	#[cfg(feature = "lmdb")]
	Lmdb(tangram_index_lmdb::Index),
}

impl Index {
	#[cfg(feature = "foundationdb")]
	pub fn new_fdb(options: &tangram_index_fdb::Options) -> tg::Result<Self> {
		Ok(Self::Fdb(tangram_index_fdb::Index::new(options)?))
	}

	#[cfg(feature = "lmdb")]
	pub fn new_lmdb(config: &tangram_index_lmdb::Config) -> tg::Result<Self> {
		Ok(Self::Lmdb(tangram_index_lmdb::Index::new(config)?))
	}
}

impl index::Index for Index {
	async fn verify_batch(
		&self,
		args: &[index::verify::Arg],
		config: index::verify::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<index::verify::Output>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.verify_batch(args, config, principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.verify_batch(args, config, principal).await,
		}
	}

	async fn contains_ids(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.contains_ids(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.contains_ids(ids).await,
		}
	}

	async fn visible(&self, ids: &[tg::Id], principal: &tg::Principal) -> tg::Result<Vec<bool>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.visible(ids, principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.visible(ids, principal).await,
		}
	}

	async fn write(&self, arg: index::write::Arg) -> tg::Result<tg::Result<()>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.write(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.write(arg).await,
		}
	}

	async fn try_get_ancestors(&self, id: &tg::Id) -> tg::Result<Option<Vec<tg::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_ancestors(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_ancestors(id).await,
		}
	}

	async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_ids_for_specifiers(specifiers).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_ids_for_specifiers(specifiers).await,
		}
	}

	async fn get_subjects_for_principal(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_subjects_for_principal(principal).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_subjects_for_principal(principal).await,
		}
	}

	async fn try_get_specifiers_for_ids(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tg::Specifier>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_specifiers_for_ids(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_specifiers_for_ids(ids).await,
		}
	}

	async fn try_get_oldest_update_transaction_id(
		&self,
		kind: index::update::Kind,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_oldest_update_transaction_id(kind).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_oldest_update_transaction_id(kind).await,
		}
	}

	async fn update_batch(
		&self,
		kind: index::update::Kind,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<index::update::Output> {
		#[cfg(not(feature = "foundationdb"))]
		let _ = (partition_start, partition_end);
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.update_batch(kind, batch_size, partition_start, partition_end)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.update_batch(kind, batch_size).await,
		}
	}

	async fn clean(&self, arg: index::clean::Arg) -> tg::Result<index::clean::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.clean(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.clean(arg).await,
		}
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_transaction_id().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_transaction_id().await,
		}
	}

	async fn sync(&self) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.sync().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.sync().await,
		}
	}

	fn cleaning_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.cleaning_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.cleaning_partition_total(),
		}
	}

	fn permission_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.permission_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.permission_update_partition_total(),
		}
	}

	fn storage_and_metadata_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.storage_and_metadata_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.storage_and_metadata_update_partition_total(),
		}
	}

	fn usage_update_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.usage_update_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.usage_update_partition_total(),
		}
	}
}
