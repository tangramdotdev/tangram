use {
	crate::prelude::*,
	futures::{FutureExt as _, Stream, stream::BoxStream},
	tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite},
};

mod module;
mod object;
mod pipe;
mod process;
mod pty;
mod remote;
mod tag;
mod user;
mod watch;

impl<L, R> tg::Handle for tg::Either<L, R>
where
	L: tg::Handle,
	R: tg::Handle,
{
	fn cache(
		&self,
		arg: tg::cache::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.cache(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.cache(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn check(&self, arg: tg::check::Arg) -> impl Future<Output = tg::Result<tg::check::Output>> {
		match self {
			tg::Either::Left(s) => s.check(arg).left_future(),
			tg::Either::Right(s) => s.check(arg).right_future(),
		}
	}

	fn checkin(
		&self,
		arg: tg::checkin::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkin::Output>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.checkin(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.checkin(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn checkout(
		&self,
		arg: tg::checkout::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::checkout::Output>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.checkout(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.checkout(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn clean(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::clean::Output>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.clean()
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.clean()
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn document(
		&self,
		arg: tg::document::Arg,
	) -> impl Future<Output = tg::Result<serde_json::Value>> {
		match self {
			tg::Either::Left(s) => s.document(arg).left_future(),
			tg::Either::Right(s) => s.document(arg).right_future(),
		}
	}

	fn format(&self, arg: tg::format::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.format(arg).left_future(),
			tg::Either::Right(s) => s.format(arg).right_future(),
		}
	}

	fn health(&self, arg: tg::health::Arg) -> impl Future<Output = tg::Result<tg::Health>> {
		match self {
			tg::Either::Left(s) => s.health(arg).left_future(),
			tg::Either::Right(s) => s.health(arg).right_future(),
		}
	}

	fn index(
		&self,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<()>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.index()
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.index()
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.lsp(input, output).left_future(),
			tg::Either::Right(s) => s.lsp(input, output).right_future(),
		}
	}

	fn pull(
		&self,
		arg: tg::pull::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::pull::Output>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.pull(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.pull(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn push(
		&self,
		arg: tg::push::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<tg::push::Output>>> + Send + 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.push(arg)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.push(arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn sync(
		&self,
		arg: tg::sync::Arg,
		stream: BoxStream<'static, tg::Result<tg::sync::Message>>,
	) -> impl Future<
		Output = tg::Result<impl Stream<Item = tg::Result<tg::sync::Message>> + Send + 'static>,
	> {
		match self {
			tg::Either::Left(s) => s
				.sync(arg, stream)
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.sync(arg, stream)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_get(
		&self,
		reference: &tg::Reference,
		arg: tg::get::Arg,
	) -> impl Future<
		Output = tg::Result<
			impl Stream<Item = tg::Result<tg::progress::Event<Option<tg::get::Output>>>>
			+ Send
			+ 'static,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_get(reference, arg.clone())
				.map(|result| result.map(futures::StreamExt::left_stream))
				.left_future(),
			tg::Either::Right(s) => s
				.try_get(reference, arg)
				.map(|result| result.map(futures::StreamExt::right_stream))
				.right_future(),
		}
	}

	fn try_read_stream(
		&self,
		arg: tg::read::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<impl Stream<Item = tg::Result<tg::read::Event>> + Send + 'static>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_read_stream(arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::left_stream)))
				.left_future(),
			tg::Either::Right(s) => s
				.try_read_stream(arg)
				.map(|result| result.map(|option| option.map(futures::StreamExt::right_stream)))
				.right_future(),
		}
	}

	fn write(
		&self,
		arg: tg::write::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> impl Future<Output = tg::Result<tg::write::Output>> {
		match self {
			tg::Either::Left(s) => s.write(arg, reader).left_future(),
			tg::Either::Right(s) => s.write(arg, reader).right_future(),
		}
	}
}
