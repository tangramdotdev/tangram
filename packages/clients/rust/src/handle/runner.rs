use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
};

pub trait Runner: Clone + Unpin + Send + Sync + 'static {
	fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::InputEvent>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::runner::control::OutputEvent>> + Send + 'static,
		>,
	> + Send;
}

impl tg::handle::Runner for tg::Client {
	async fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::InputEvent>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::runner::control::OutputEvent>> + Send + 'static>
	{
		self.session(&self.context)
			.get_runner_control_stream(id, arg, stream)
			.await
	}
}
