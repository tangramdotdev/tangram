use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn enqueue_permission_capture(
		&self,
		arg: index::permission::capture::enqueue::Arg,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.enqueue_permission_capture(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.enqueue_permission_capture(arg).await,
		}
	}

	pub async fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<index::permission::capture::Entry>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.permission_capture_batch(batch_size, partition_start, partition_end)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.permission_capture_batch(batch_size, partition_start, partition_end)
					.await
			},
		}
	}

	pub async fn complete_permission_capture(
		&self,
		entry: &index::permission::capture::Entry,
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.complete_permission_capture(entry).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.complete_permission_capture(entry).await,
		}
	}

	pub async fn put_permissions(&self, args: &[index::permission::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_permissions(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_permissions(args).await,
		}
	}

	pub async fn delete_permissions(
		&self,
		args: &[index::permission::delete::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_permissions(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_permissions(args).await,
		}
	}
}

impl index::permission::Index for Index {
	async fn enqueue_permission_capture(
		&self,
		arg: index::permission::capture::enqueue::Arg,
	) -> tg::Result<()> {
		self.enqueue_permission_capture(arg).await
	}

	async fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<index::permission::capture::Entry>> {
		self.permission_capture_batch(batch_size, partition_start, partition_end)
			.await
	}

	async fn complete_permission_capture(
		&self,
		entry: &index::permission::capture::Entry,
	) -> tg::Result<()> {
		self.complete_permission_capture(entry).await
	}

	async fn put_permissions(&self, args: &[index::permission::put::Arg]) -> tg::Result<()> {
		self.put_permissions(args).await
	}

	async fn delete_permissions(&self, args: &[index::permission::delete::Arg]) -> tg::Result<()> {
		self.delete_permissions(args).await
	}
}
