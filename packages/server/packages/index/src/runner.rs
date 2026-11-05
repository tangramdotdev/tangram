use {super::Index, tangram_client::prelude::*};

impl Index {
	pub async fn try_get_runner(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Option<tangram_index::runner::Runner>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_runner(runner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_runner(runner).await,
		}
	}

	pub async fn next_runner_registration_index(&self, runner: &tg::runner::Id) -> tg::Result<u64> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.next_runner_registration_index(runner).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.next_runner_registration_index(runner).await,
		}
	}
}
