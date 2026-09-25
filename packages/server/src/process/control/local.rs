use {
	crate::control,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream::BoxStream},
	tangram_client::{
		prelude::*,
		process::stdio::{flow, read},
	},
	tangram_futures::stream::Ext as _,
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub(crate) struct Local {
	sender: tokio::sync::mpsc::Sender<Message>,
}

pub(crate) enum Message {
	Close(String),
	Progress(tg::process::control::ReadServerNotification),
	Request {
		request: tg::process::control::ServerRequest,
		sender: Reply,
	},
}

#[derive(Clone)]
pub(crate) enum Reply {
	Local(tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>),
	Remote(
		control::Sender<tg::process::control::ServerMessage, tg::process::control::ClientMessage>,
	),
}

struct State {
	control_sender: Local,
	finished: bool,
	id: String,
	pending: usize,
	receiver: tokio::sync::mpsc::Receiver<tg::process::control::ClientMessage>,
	window: flow::Receiver,
}

impl Local {
	#[must_use]
	pub(crate) fn new() -> (Self, tokio::sync::mpsc::Receiver<Message>) {
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
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

	pub(crate) fn read(
		&self,
		arg: read::Arg,
	) -> BoxStream<'static, tg::Result<read::ServerMessage>> {
		let id = control::id();
		let (sender, receiver) = tokio::sync::mpsc::channel(flow::CHANNEL_CAPACITY);
		let request = tg::process::control::ServerRequest {
			arg: tg::process::control::ServerRequestArg::Read(arg),
			id: id.clone(),
		};
		let message = Message::Request {
			request,
			sender: Reply::Local(sender),
		};
		let guard = scopeguard::guard((self.clone(), id.clone()), |(control_sender, id)| {
			tokio::spawn(async move {
				control_sender.sender.send(Message::Close(id)).await.ok();
			});
		});
		let control_sender = self.clone();
		let stream = futures::stream::once(async move {
			control_sender
				.sender
				.send(message)
				.await
				.map_err(|_| tg::error!("the runner control channel closed"))?;
			let state = State {
				control_sender,
				finished: false,
				id,
				pending: 0,
				receiver,
				window: flow::Receiver::default(),
			};
			let stream = futures::stream::try_unfold(state, State::next);
			Ok::<_, tg::Error>(stream)
		});
		stream.try_flatten().attach(guard).boxed()
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
		// The read flow window bounds the queue; abandoned callers must not stop the runner.
		match sender.try_send(message) {
			Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => (),
			Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
				return Err(tg::error!("the runner response window was exceeded"));
			},
		}
		Ok(())
	}
}

impl State {
	async fn next(mut self) -> tg::Result<Option<(read::ServerMessage, Self)>> {
		if self.finished {
			return Ok(None);
		}
		if let Some(progress) = self.window.consume(self.pending)? {
			let notification = tg::process::control::ReadServerNotification {
				id: self.id.clone(),
				progress,
			};
			// The handler may have retired after queueing the final response.
			self.control_sender
				.sender
				.send(Message::Progress(notification))
				.await
				.ok();
		}
		let message = self
			.receiver
			.recv()
			.await
			.ok_or_else(|| tg::error!("the runner read response channel closed"))?;
		let message = match message {
			tg::process::control::ClientMessage::Notification(
				tg::process::control::ClientNotification::Read(notification),
			) => read::ServerMessage::Notification(notification.event),
			tg::process::control::ClientMessage::Response(response) => {
				read::ServerMessage::Response(
					response_output(response)?
						.try_unwrap_read()
						.map_err(|_| tg::error!("expected a runner read response"))?,
				)
			},
			_ => return Err(tg::error!("expected a runner read message")),
		};
		self.pending = match &message {
			read::ServerMessage::Notification(read::Event::Chunk(chunk)) => chunk.bytes.len(),
			_ => 0,
		};
		self.finished = matches!(message, read::ServerMessage::Response(_));

		Ok(Some((message, self)))
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
