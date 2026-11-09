use {
	crate::Component,
	std::sync::{Arc, RwLock},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index as index,
	tangram_server_core::Server,
};

#[derive(Clone, Default)]
pub(crate) struct Cache(Arc<RwLock<Vec<tg::scheduler::Id>>>);

impl Component {
	pub(crate) async fn scheduler_cache_task(
		&self,
		server: &Server,
		poll_interval: std::time::Duration,
		stopper: Stopper,
	) {
		if !server.config.advanced.multi_server {
			return;
		}
		let mut interval = tokio::time::interval(poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => break,
				_ = interval.tick() => {
					if let Err(error) = self.refresh_scheduler_cache(server).await {
						tracing::error!(error = %error.trace(), "failed to refresh the scheduler cache");
					}
				},
			}
		}
	}

	pub(crate) async fn get_schedulers(
		server: &Server,
	) -> tg::Result<Vec<index::scheduler::Scheduler>> {
		server
			.index
			.get_schedulers()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the schedulers"))
	}

	pub(crate) async fn refresh_scheduler_cache(&self, server: &Server) -> tg::Result<()> {
		let schedulers = Self::get_schedulers(server).await?;
		let schedulers = schedulers
			.into_iter()
			.filter_map(|scheduler| scheduler.available.then_some(scheduler.id))
			.collect();
		self.schedulers.replace(schedulers);

		Ok(())
	}

	pub(crate) async fn select_scheduler(
		&self,
		server: &Server,
		index: u64,
		refresh: bool,
	) -> tg::Result<tg::scheduler::Id> {
		let mut schedulers = self.schedulers.available();
		if refresh || schedulers.is_empty() {
			self.refresh_scheduler_cache(server).await?;
			schedulers = self.schedulers.available();
		}
		let len = u64::try_from(schedulers.len()).unwrap();
		if len == 0 {
			return Err(tg::error!("no schedulers are available"));
		}
		let index = usize::try_from(index % len).unwrap();
		let scheduler = schedulers.swap_remove(index);

		Ok(scheduler)
	}
}

impl Cache {
	#[must_use]
	pub(crate) fn available(&self) -> Vec<tg::scheduler::Id> {
		self.0
			.read()
			.expect("failed to read the scheduler cache")
			.clone()
	}

	pub(crate) fn replace(&self, schedulers: Vec<tg::scheduler::Id>) {
		*self.0.write().expect("failed to write the scheduler cache") = schedulers;
	}
}
