use {
	crate::prelude::*,
	futures::{FutureExt as _, future::BoxFuture},
};

pub trait Checkpoint: Send + Sync + 'static {
	fn try_watch_checkpoint<'a>(
		&'a self,
		checkpoint: &'a str,
		arg: tg::checkpoint::watch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::watch::Output>>>;

	fn try_wait_checkpoint_hit<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::wait::Output>>>;

	fn try_continue_checkpoint_hit<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_unwatch_checkpoint<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;
}

impl<T> Checkpoint for T
where
	T: tg::handle::Checkpoint,
{
	fn try_watch_checkpoint<'a>(
		&'a self,
		checkpoint: &'a str,
		arg: tg::checkpoint::watch::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::watch::Output>>> {
		self.try_watch_checkpoint(checkpoint, arg).boxed()
	}

	fn try_wait_checkpoint_hit<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<tg::checkpoint::wait::Output>>> {
		self.try_wait_checkpoint_hit(checkpoint, watch, hit).boxed()
	}

	fn try_continue_checkpoint_hit<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
		hit: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_continue_checkpoint_hit(checkpoint, watch, hit)
			.boxed()
	}

	fn try_unwatch_checkpoint<'a>(
		&'a self,
		checkpoint: &'a str,
		watch: u64,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_unwatch_checkpoint(checkpoint, watch).boxed()
	}
}
