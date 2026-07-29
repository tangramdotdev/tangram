use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Checkpoint for Handle {
	fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::watch::Output>>> {
		// SAFETY: The erased future borrows the handle and checkpoint for the returned future's lifetime.
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_watch_checkpoint(checkpoint, arg))
		}
	}

	fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::wait::Output>>> {
		// SAFETY: The erased future borrows the handle and checkpoint for the returned future's lifetime.
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_wait_checkpoint_hit(checkpoint, watch, hit),
			)
		}
	}

	fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		// SAFETY: The erased future borrows the handle and checkpoint for the returned future's lifetime.
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_continue_checkpoint_hit(checkpoint, watch, hit),
			)
		}
	}

	fn try_unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		// SAFETY: The erased future borrows the handle and checkpoint for the returned future's lifetime.
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.try_unwatch_checkpoint(checkpoint, watch),
			)
		}
	}
}
