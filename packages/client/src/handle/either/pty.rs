use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream},
};

impl<L, R> tg::handle::Pty for tg::Either<L, R>
where
	L: tg::handle::Pty,
	R: tg::handle::Pty,
{
	fn create_pty(
		&self,
		arg: tg::pty::create::Arg,
	) -> impl Future<Output = tg::Result<tg::pty::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_pty(arg).left_future(),
			tg::Either::Right(s) => s.create_pty(arg).right_future(),
		}
	}

	fn close_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::close::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.close_pty(id, arg).left_future(),
			tg::Either::Right(s) => s.close_pty(id, arg).right_future(),
		}
	}

	fn delete_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_pty(id, arg).left_future(),
			tg::Either::Right(s) => s.delete_pty(id, arg).right_future(),
		}
	}

	fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::pty::Size>>> {
		match self {
			tg::Either::Left(s) => s.get_pty_size(id, arg).left_future(),
			tg::Either::Right(s) => s.get_pty_size(id, arg).right_future(),
		}
	}

	fn put_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::size::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_pty_size(id, arg).left_future(),
			tg::Either::Right(s) => s.put_pty_size(id, arg).right_future(),
		}
	}

	fn try_read_pty_stream(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_read_pty_stream(id, arg)
				.map(|result| result.map(|opt| opt.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_read_pty_stream(id, arg)
				.map(|result| result.map(|opt| opt.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn write_pty(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::write::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.write_pty(id, arg).left_future(),
			tg::Either::Right(s) => s.write_pty(id, arg).right_future(),
		}
	}
}
