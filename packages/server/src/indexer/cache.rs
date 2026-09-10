use {
	crate::Server,
	std::sync::{Arc, RwLock},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::{self as index, Index as _},
};

#[derive(Clone, Default)]
pub(crate) struct Cache(Arc<RwLock<Vec<tg::indexer::Id>>>);

impl Cache {
	#[must_use]
	pub fn available(&self) -> Vec<tg::indexer::Id> {
		self.0
			.read()
			.expect("failed to read the indexer cache")
			.clone()
	}

	pub fn replace(&self, indexers: Vec<tg::indexer::Id>) {
		*self.0.write().expect("failed to write the indexer cache") = indexers;
	}
}

impl Server {
	pub(crate) async fn indexer_cache_task(
		&self,
		poll_interval: std::time::Duration,
		stopper: Stopper,
	) {
		if self.config.advanced.single_process {
			return;
		}
		let mut interval = tokio::time::interval(poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				_ = interval.tick() => {
					if let Err(error) = self.refresh_indexer_cache().await {
						tracing::error!(error = %error.trace(), "failed to refresh the indexer cache");
					}
				},
			}
		}
	}

	pub(crate) async fn get_indexers(&self) -> tg::Result<Vec<index::indexer::Indexer>> {
		self.index
			.get_indexers()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the indexers"))
	}

	pub(crate) async fn refresh_indexer_cache(&self) -> tg::Result<()> {
		let indexers = self.get_indexers().await?;
		let indexers = indexers
			.into_iter()
			.filter_map(|indexer| indexer.available.then_some(indexer.id))
			.collect();
		self.indexers.replace(indexers);

		Ok(())
	}

	pub(crate) async fn select_indexer(
		&self,
		index: u64,
		refresh: bool,
	) -> tg::Result<tg::indexer::Id> {
		let mut indexers = self.indexers.available();
		if refresh || indexers.is_empty() {
			self.refresh_indexer_cache().await?;
			indexers = self.indexers.available();
		}
		let len = u64::try_from(indexers.len()).unwrap();
		if len == 0 {
			return Err(tg::error!("no indexers are available"));
		}
		let index = usize::try_from(index % len).unwrap();
		let indexer = indexers.swap_remove(index);

		Ok(indexer)
	}
}
