use {super::Index, std::time::Duration, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_processes(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_processes(ids).await,
		}
	}

	pub async fn try_get_process_children_count(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_process_children_count(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_process_children_count(id).await,
		}
	}

	pub async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_process_children(id, position, length).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_process_children(id, position, length).await,
		}
	}

	pub async fn try_get_process_children_and_objects(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::NodeChildren>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_process_children_and_objects(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_process_children_and_objects(id).await,
		}
	}

	pub async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, index::process::Process)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_cached_processes(command).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_cached_processes(command).await,
		}
	}

	pub async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.process_has_ancestor(process, ancestor).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.process_has_ancestor(process, ancestor).await,
		}
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.touch_processes(ids, touched_at, time_to_touch).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.touch_processes(ids, touched_at, time_to_touch).await,
		}
	}

	pub async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &index::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.touch_processes_and_put_account(ids, account, touched_at, time_to_touch)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.touch_processes_and_put_account(ids, account, touched_at, time_to_touch)
					.await
			},
		}
	}

	pub async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => {
				index
					.touch_processes_with_account(ids, account, touched_at, time_to_touch)
					.await
			},
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => {
				index
					.touch_processes_with_account(ids, account, touched_at, time_to_touch)
					.await
			},
		}
	}
}

impl index::process::Index for Index {
	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		self.try_get_processes(ids).await
	}

	async fn try_get_process_children_count(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<u64>> {
		self.try_get_process_children_count(id).await
	}

	async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		self.try_get_process_children(id, position, length).await
	}

	async fn try_get_process_children_and_objects(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::NodeChildren>> {
		self.try_get_process_children_and_objects(id).await
	}

	async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, index::process::Process)>> {
		self.try_get_cached_processes(command).await
	}

	async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		self.process_has_ancestor(process, ancestor).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		self.touch_processes(ids, touched_at, time_to_touch).await
	}

	async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &index::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		self.touch_processes_and_put_account(ids, account, touched_at, time_to_touch)
			.await
	}

	async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<index::process::Process>>> {
		self.touch_processes_with_account(ids, account, touched_at, time_to_touch)
			.await
	}
}
