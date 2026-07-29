use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Checkpoint for tg::Either<L, R>
where
	L: tg::handle::Checkpoint,
	R: tg::handle::Checkpoint,
{
	fn try_watch_checkpoint(
		&self,
		checkpoint: &str,
		arg: tg::checkpoint::watch::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::watch::Output>>> {
		match self {
			tg::Either::Left(handle) => handle.try_watch_checkpoint(checkpoint, arg).left_future(),
			tg::Either::Right(handle) => {
				handle.try_watch_checkpoint(checkpoint, arg).right_future()
			},
		}
	}

	fn try_wait_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<tg::checkpoint::wait::Output>>> {
		match self {
			tg::Either::Left(handle) => handle
				.try_wait_checkpoint_hit(checkpoint, watch, hit)
				.left_future(),
			tg::Either::Right(handle) => handle
				.try_wait_checkpoint_hit(checkpoint, watch, hit)
				.right_future(),
		}
	}

	fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(handle) => handle
				.try_continue_checkpoint_hit(checkpoint, watch, hit)
				.left_future(),
			tg::Either::Right(handle) => handle
				.try_continue_checkpoint_hit(checkpoint, watch, hit)
				.right_future(),
		}
	}

	fn try_unwatch_checkpoint(
		&self,
		checkpoint: &str,
		watch: u64,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(handle) => handle
				.try_unwatch_checkpoint(checkpoint, watch)
				.left_future(),
			tg::Either::Right(handle) => handle
				.try_unwatch_checkpoint(checkpoint, watch)
				.right_future(),
		}
	}
}
