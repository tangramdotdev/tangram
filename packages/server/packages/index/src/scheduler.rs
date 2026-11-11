use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn get_schedulers(&self) -> tg::Result<Vec<index::scheduler::Scheduler>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.get_schedulers().await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.get_schedulers().await,
		}
	}

	pub async fn put_scheduler(&self, arg: index::scheduler::put::Arg) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_scheduler(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_scheduler(arg).await,
		}
	}

	pub async fn try_get_scheduler(
		&self,
		arg: index::scheduler::get::Arg,
	) -> tg::Result<Option<index::scheduler::Scheduler>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_scheduler(arg).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_scheduler(arg).await,
		}
	}

	/// Get the scheduler that holds the latest boot of a sandbox that a start reserved or a scheduler accepted, until its runner connects.
	pub async fn try_get_sandbox_scheduler(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::scheduler::Id>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_sandbox_scheduler(sandbox).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_sandbox_scheduler(sandbox).await,
		}
	}
}

impl index::scheduler::Index for Index {
	async fn get_schedulers(&self) -> tg::Result<Vec<index::scheduler::Scheduler>> {
		self.get_schedulers().await
	}

	async fn put_scheduler(&self, arg: index::scheduler::put::Arg) -> tg::Result<()> {
		self.put_scheduler(arg).await
	}

	async fn try_get_scheduler(
		&self,
		arg: index::scheduler::get::Arg,
	) -> tg::Result<Option<index::scheduler::Scheduler>> {
		self.try_get_scheduler(arg).await
	}
}
