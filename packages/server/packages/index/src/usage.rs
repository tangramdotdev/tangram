use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn expire_usage(
		&self,
		arg: index::usage::expire::Arg,
	) -> tg::Result<index::usage::expire::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.expire_usage(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.expire_usage(arg).await,
		}
	}

	pub async fn aggregate_usage(
		&self,
		arg: index::usage::aggregate::Arg,
	) -> tg::Result<index::usage::aggregate::Output> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.aggregate_usage(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.aggregate_usage(arg).await,
		}
	}

	pub async fn get_usage(
		&self,
		account: &index::usage::Account,
		period: index::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<index::usage::Aggregate> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_usage(account, period, now).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_usage(account, period, now).await,
		}
	}

	pub async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.start_usage(at).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.start_usage(at).await,
		}
	}

	#[must_use]
	pub fn usage_partition_total(&self) -> u64 {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.usage_partition_total(),
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.usage_partition_total(),
		}
	}
}

impl index::usage::Index for Index {
	async fn expire_usage(
		&self,
		arg: index::usage::expire::Arg,
	) -> tg::Result<index::usage::expire::Output> {
		self.expire_usage(arg).await
	}

	async fn aggregate_usage(
		&self,
		arg: index::usage::aggregate::Arg,
	) -> tg::Result<index::usage::aggregate::Output> {
		self.aggregate_usage(arg).await
	}

	async fn get_usage(
		&self,
		account: &index::usage::Account,
		period: index::usage::Period,
		now: jiff::Timestamp,
	) -> tg::Result<index::usage::Aggregate> {
		self.get_usage(account, period, now).await
	}

	async fn start_usage(&self, at: jiff::Timestamp) -> tg::Result<()> {
		self.start_usage(at).await
	}

	fn usage_partition_total(&self) -> u64 {
		self.usage_partition_total()
	}
}
