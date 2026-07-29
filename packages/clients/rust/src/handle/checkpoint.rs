use crate::prelude::*;

pub trait Checkpoint: Clone + Unpin + Send + Sync + 'static {
	fn watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> impl Future<Output = tg::Result<tg::checkpoint::watch::Output>> + Send {
		async move {
			self.try_watch_checkpoint(checkpoint, arg)
				.await?
				.ok_or_else(|| tg::error!("checkpoints are not enabled"))
		}
	}

	fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::watch::Output>>> + Send;

	fn wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<tg::checkpoint::wait::Output>> + Send {
		async move {
			self.try_wait_checkpoint_hit(checkpoint, watch, hit)
				.await?
				.ok_or_else(|| tg::error!("failed to find the checkpoint watch"))
		}
	}

	fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::wait::Output>>> + Send;

	fn continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_continue_checkpoint_hit(checkpoint, watch, hit)
				.await?
				.ok_or_else(|| tg::error!("failed to find the checkpoint hit"))
		}
	}

	fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_unwatch_checkpoint(checkpoint, watch)
				.await?
				.ok_or_else(|| tg::error!("failed to find the checkpoint watch"))
		}
	}

	fn try_unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Checkpoint for tg::Client {
	async fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> tg::Result<Option<tg::checkpoint::watch::Output>> {
		self.session(&self.context)
			.try_watch_checkpoint(checkpoint, arg)
			.await
	}

	async fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<tg::checkpoint::wait::Output>> {
		self.session(&self.context)
			.try_wait_checkpoint_hit(checkpoint, watch, hit)
			.await
	}

	async fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_continue_checkpoint_hit(checkpoint, watch, hit)
			.await
	}

	async fn try_unwatch_checkpoint(&self, checkpoint: &str, watch: u64) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_unwatch_checkpoint(checkpoint, watch)
			.await
	}
}
