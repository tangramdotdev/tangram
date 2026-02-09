use {crate::prelude::*, futures::Stream};

pub trait Pipe: Clone + Unpin + Send + Sync + 'static {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> + Send;

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_read_pipe_stream(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::pipe::Event>> + Send + 'static>,
		>,
	> + Send;

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl tg::handle::Pipe for tg::Client {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		self.create_pipe(arg)
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.close_pipe(id, arg)
	}

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.delete_pipe(id, arg)
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
		self.try_read_pipe_stream(id, arg)
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.write_pipe(id, arg)
	}
}
