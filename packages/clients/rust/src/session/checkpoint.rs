use crate::prelude::*;

impl tg::handle::Checkpoint for tg::Session {
	fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::watch::Output>>> {
		self.try_watch_checkpoint(checkpoint, arg)
	}

	fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::wait::Output>>> {
		self.try_wait_checkpoint_hit(checkpoint, watch, hit)
	}

	fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_continue_checkpoint_hit(checkpoint, watch, hit)
	}

	fn try_unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_unwatch_checkpoint(checkpoint, watch)
	}

	fn try_abort_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::abort::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_abort_checkpoint(checkpoint, arg)
	}

	fn try_panic_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::panic::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_panic_checkpoint(checkpoint, arg)
	}
}
