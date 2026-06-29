use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Runner: Send + Sync + 'static {
	fn get_runner_control_stream<'a>(
		&'a self,
		id: &'a tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>>>;
}

impl<T> Runner for T
where
	T: tg::handle::Runner,
{
	fn get_runner_control_stream<'a>(
		&'a self,
		id: &'a tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::ClientMessage>>,
	) -> BoxFuture<'a, tg::Result<BoxStream<'static, tg::Result<tg::runner::control::ServerMessage>>>>
	{
		self.get_runner_control_stream(id, arg, stream)
			.map_ok(futures::StreamExt::boxed)
			.boxed()
	}
}
