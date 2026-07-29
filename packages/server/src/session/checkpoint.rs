use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Checkpoint for Session {
	async fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> tg::Result<Option<tg::checkpoint::watch::Output>> {
		self.try_watch_checkpoint(checkpoint, arg).await
	}

	async fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<tg::checkpoint::wait::Output>> {
		self.try_wait_checkpoint_hit(checkpoint, watch, hit).await
	}

	async fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<()>> {
		self.try_continue_checkpoint_hit(checkpoint, watch, hit)
			.await
	}

	async fn try_unwatch_checkpoint(&self, checkpoint: &str, watch: u64) -> tg::Result<Option<()>> {
		self.try_unwatch_checkpoint(checkpoint, watch).await
	}
}
