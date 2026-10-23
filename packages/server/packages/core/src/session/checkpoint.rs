use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Checkpoint for Session {
	async fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> tg::Result<Option<tg::checkpoint::watch::Output>> {
		self.server
			.components
			.checkpoint
			.try_watch_checkpoint(self, checkpoint, arg)
			.await
	}

	async fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<tg::checkpoint::wait::Output>> {
		self.server
			.components
			.checkpoint
			.try_wait_checkpoint_hit(self, checkpoint, watch, hit)
			.await
	}

	async fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.checkpoint
			.try_continue_checkpoint_hit(self, checkpoint, watch, hit)
			.await
	}

	async fn try_unwatch_checkpoint(&self, checkpoint: &str, watch: u64) -> tg::Result<Option<()>> {
		self.server
			.components
			.checkpoint
			.try_unwatch_checkpoint(self, checkpoint, watch)
			.await
	}

	async fn try_abort_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::abort::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.checkpoint
			.try_abort_checkpoint(self, checkpoint, arg)
			.await
	}

	async fn try_panic_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::panic::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.checkpoint
			.try_panic_checkpoint(self, checkpoint, arg)
			.await
	}
}
