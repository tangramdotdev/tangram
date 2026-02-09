use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream},
};

impl<L, R> tg::handle::Pipe for tg::Either<L, R>
where
	L: tg::handle::Pipe,
	R: tg::handle::Pipe,
{
	fn create_pipe(
		&self,
		arg: tg::pipe::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pipe::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_pipe(arg).left_future(),
			tg::Either::Right(s) => s.create_pipe(arg).right_future(),
		}
	}

	fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.close_pipe(id, arg).left_future(),
			tg::Either::Right(s) => s.close_pipe(id, arg).right_future(),
		}
	}

	fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_pipe(id, arg).left_future(),
			tg::Either::Right(s) => s.delete_pipe(id, arg).right_future(),
		}
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
		match self {
			tg::Either::Left(s) => s
				.try_read_pipe_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_read_pipe_stream(id, arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn write_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.write_pipe(id, arg).left_future(),
			tg::Either::Right(s) => s.write_pipe(id, arg).right_future(),
		}
	}
}
