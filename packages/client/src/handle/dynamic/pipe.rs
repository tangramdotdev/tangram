use {
	super::Handle,
	crate::prelude::*,
	futures::{Stream, future::BoxFuture, stream::BoxStream},
};

impl tg::handle::Pipe for Handle {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		self.0.create_pipe(arg)
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.close_pipe(id, arg)) }
	}

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.delete_pipe(id, arg)) }
	}

	fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>,
		>,
	> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, tg::Result<Option<BoxStream<_>>>>>(
				self.0.try_read_pipe_stream(id, arg),
			)
		}
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.write_pipe(id, arg)) }
	}
}
