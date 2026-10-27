use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes_for_creator(creator).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes_for_creator(creator).await,
		}
	}

	pub async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes_for_owner(owner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes_for_owner(owner).await,
		}
	}

	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_runner_sandboxes(runner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_runner_sandboxes(runner).await,
		}
	}

	pub async fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_sandbox_processes_count(id).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_sandbox_processes_count(id).await,
		}
	}

	pub async fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_sandbox_processes(id, position, length).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_sandbox_processes(id, position, length).await,
		}
	}

	pub async fn list_sandboxes(
		&self,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.list_sandboxes().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.list_sandboxes().await,
		}
	}

	pub async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<index::sandbox::Sandbox>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_sandboxes(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_sandboxes(ids).await,
		}
	}
}

impl index::sandbox::Index for Index {
	async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		self.list_sandboxes_for_creator(creator).await
	}

	async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		self.list_sandboxes_for_owner(owner).await
	}

	async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		self.get_runner_sandboxes(runner).await
	}

	async fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		self.try_get_sandbox_processes_count(id).await
	}

	async fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		self.try_get_sandbox_processes(id, position, length).await
	}

	async fn list_sandboxes(&self) -> tg::Result<Vec<(tg::sandbox::Id, index::sandbox::Sandbox)>> {
		self.list_sandboxes().await
	}

	async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<index::sandbox::Sandbox>>> {
		self.try_get_sandboxes(ids).await
	}
}
