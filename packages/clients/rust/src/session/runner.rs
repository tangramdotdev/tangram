use {
	crate::prelude::*,
	futures::{Stream, stream::BoxStream},
};

impl tg::handle::Runner for tg::Session {
	fn get_runner_control_stream(
		&self,
		id: &tg::runner::Id,
		arg: tg::runner::control::Arg,
		stream: BoxStream<'static, tg::Result<tg::runner::control::InputEvent>>,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::runner::control::OutputEvent>> + Send + 'static,
		>,
	> {
		self.get_runner_control_stream(id, arg, stream)
	}
}
