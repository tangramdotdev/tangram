use {
	crate::control,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tg::process::stdio::{read, write},
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct Local {
	sender: tokio::sync::mpsc::Sender<Message>,
}

pub(crate) enum Message {
	Ack {
		position: u64,
		streams: Vec<tg::process::stdio::Stream>,
	},
	Read {
		arg: read::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<read::ServerMessage>>,
	},
	Request {
		request: tg::process::control::ServerRequest,
		sender: Reply,
	},
	Write {
		data: write::Data,
		sender: tokio::sync::oneshot::Sender<tg::Result<write::Output>>,
	},
}

#[derive(Clone)]
pub(crate) enum Reply {
	Local(tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>),
	Remote(tg::process::control::Sender),
}

impl Local {
	#[must_use]
	pub(crate) fn new(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<Message>) {
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		(Self { sender }, receiver)
	}

	pub(crate) async fn request(
		&self,
		arg: tg::process::control::ServerRequestArg,
	) -> tg::Result<tg::process::control::ClientResponseOutput> {
		self.send_request(arg).await?.await?
	}

	pub(crate) async fn send_request(
		&self,
		arg: tg::process::control::ServerRequestArg,
	) -> tg::Result<
		futures::future::BoxFuture<
			'static,
			tg::Result<tg::Result<tg::process::control::ClientResponseOutput>>,
		>,
	> {
		let id = control::id();
		let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
		let request = tg::process::control::ServerRequest { arg, id };
		let message = Message::Request {
			request,
			sender: Reply::Local(sender),
		};
		self.sender
			.send(message)
			.await
			.map_err(|_| tg::error!("the runner control channel closed"))?;
		let future = async move {
			let message = receiver
				.recv()
				.await
				.ok_or_else(|| tg::error!("the runner control response channel closed"))?;
			let tg::process::control::ClientMessage::Response(response) = message else {
				return Err(tg::error!("expected a runner control response"));
			};
			Ok(response_output(response))
		}
		.boxed();
		Ok(future)
	}

	pub(crate) async fn write(
		&self,
		data: write::Data,
	) -> tg::Result<futures::future::BoxFuture<'static, tg::Result<write::Output>>> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message::Write { data, sender };
		self.sender
			.send(message)
			.await
			.map_err(|_| tg::error!("the runner control channel closed"))?;
		let future = async move {
			receiver
				.await
				.map_err(|_| tg::error!("the runner stdin response channel closed"))?
		}
		.boxed();
		Ok(future)
	}

	pub(crate) fn read(
		&self,
		mut arg: read::Arg,
		mut input: BoxStream<'static, tg::Result<read::ClientMessage>>,
	) -> BoxStream<'static, tg::Result<read::ServerMessage>> {
		// Normalize the streams, so that the acknowledgments use the read's coordinates.
		arg.streams.sort();
		arg.streams.dedup();

		// Forward the reader's acknowledgments, so the runner releases the bytes it has.
		let streams = arg.streams.clone();
		let ack_task = Task::spawn({
			let control_sender = self.clone();
			move |_| async move {
				while let Some(Ok(read::ClientMessage::Ack { position })) = input.next().await {
					let streams = streams.clone();
					let message = Message::Ack { position, streams };
					if control_sender.sender.send(message).await.is_err() {
						break;
					}
				}
			}
		});

		// The bounded channel holds back the runner, and dropping the receiver ends the read.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let message = Message::Read { arg, sender };
		let control_sender = self.clone();
		futures::stream::once(async move {
			control_sender
				.sender
				.send(message)
				.await
				.map_err(|_| tg::error!("the runner control channel closed"))?;
			let stream = futures::stream::try_unfold(
				(receiver, false),
				|(mut receiver, finished)| async move {
					if finished {
						return Ok(None);
					}
					let message = receiver
						.recv()
						.await
						.ok_or_else(|| tg::error!("the runner read channel closed"))??;
					let finished = matches!(message, read::ServerMessage::Response(_));
					Ok(Some((message, (receiver, finished))))
				},
			);
			Ok::<_, tg::Error>(stream)
		})
		.try_flatten()
		.attach(ack_task)
		.boxed()
	}
}

impl Reply {
	pub(crate) fn send(
		&self,
		message: tg::process::control::ClientMessage,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Self::Local(sender) => futures::future::Either::Left(futures::future::ready(
				Self::send_local(sender, message),
			)),
			Self::Remote(sender) => futures::future::Either::Right(sender.send(message)),
		}
	}

	pub(crate) fn send_low(
		&self,
		message: tg::process::control::ClientMessage,
	) -> impl Future<Output = tg::Result<()>> + Send {
		match self {
			Self::Local(sender) => futures::future::Either::Left(futures::future::ready(
				Self::send_local(sender, message),
			)),
			Self::Remote(sender) => futures::future::Either::Right(sender.send_low(message)),
		}
	}

	fn send_local(
		sender: &tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
		message: tg::process::control::ClientMessage,
	) -> tg::Result<()> {
		// Each request has a single response, and abandoned callers must not stop the runner.
		match sender.try_send(message) {
			Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => (),
			Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
				return Err(tg::error!("the runner response window was exceeded"));
			},
		}
		Ok(())
	}
}

fn response_output(
	response: tg::process::control::ClientResponse,
) -> tg::Result<tg::process::control::ClientResponseOutput> {
	if let Some(error) = response.error {
		return Err(tg::Error::try_from(error)?);
	}
	response
		.output
		.ok_or_else(|| tg::error!("missing the runner control response output"))
}
