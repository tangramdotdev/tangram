use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*, stream::BoxStream},
};

pub trait Pipe: Send + Sync + 'static {
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pipe::create::Output>>;

	fn close_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_read_pipe_stream<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pipe::Event>>>>>;

	fn write_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;
}

impl<T> Pipe for T
where
	T: tg::handle::Pipe,
{
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::pipe::create::Output>> {
		self.create_pipe(arg).boxed()
	}

	fn close_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.close_pipe(id, arg).boxed()
	}

	fn delete_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_pipe(id, arg).boxed()
	}

	fn try_read_pipe_stream<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::read::Arg,
	) -> BoxFuture<'a, tg::Result<Option<BoxStream<'static, tg::Result<tg::pipe::Event>>>>> {
		self.try_read_pipe_stream(id, arg)
			.map_ok(|option| option.map(futures::StreamExt::boxed))
			.boxed()
	}

	fn write_pipe<'a>(
		&'a self,
		id: &'a tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.write_pipe(id, arg).boxed()
	}
}
