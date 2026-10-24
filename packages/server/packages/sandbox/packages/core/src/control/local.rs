use {futures::FutureExt as _, tangram_client::prelude::*, tangram_server_control as control};

#[cfg(test)]
mod tests;

#[derive(Clone)]
pub struct Local {
	sender: tokio::sync::mpsc::Sender<Message>,
}

pub struct Message {
	pub arg: tg::sandbox::control::ServerRequestArg,
	pub sender: Reply,
}

pub enum Reply {
	Local(tokio::sync::oneshot::Sender<tg::Result<tg::sandbox::control::ClientResponseOutput>>),
	Remote {
		id: String,
		sender: control::Sender<
			tg::sandbox::control::ServerMessage,
			tg::sandbox::control::ClientMessage,
		>,
	},
}

impl Local {
	#[must_use]
	pub fn new(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<Message>) {
		let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
		(Self { sender }, receiver)
	}

	#[must_use]
	pub fn is_closed(&self) -> bool {
		self.sender.is_closed()
	}

	pub async fn request(
		&self,
		arg: tg::sandbox::control::ServerRequestArg,
	) -> tg::Result<tg::sandbox::control::ClientResponseOutput> {
		self.send_request(arg).await?.await?
	}

	pub async fn send_request(
		&self,
		arg: tg::sandbox::control::ServerRequestArg,
	) -> tg::Result<
		futures::future::BoxFuture<
			'static,
			tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>>,
		>,
	> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message {
			arg,
			sender: Reply::Local(sender),
		};
		self.sender
			.send(message)
			.await
			.map_err(|_| tg::error!("the runner control channel closed"))?;
		let future = async move {
			receiver
				.await
				.map_err(|_| tg::error!("the runner control response channel closed"))
		}
		.boxed();
		Ok(future)
	}
}

impl Reply {
	pub async fn send(
		self,
		result: tg::Result<tg::sandbox::control::ClientResponseOutput>,
	) -> tg::Result<()> {
		match self {
			Self::Local(sender) => {
				// An abandoned caller must not stop the sandbox.
				sender.send(result).ok();
			},
			Self::Remote { id, sender } => {
				let (error, output) = match result {
					Ok(output) => (None, Some(output)),
					Err(error) => (
						Some(tg::error::Data {
							message: Some(error.to_string()),
							..Default::default()
						}),
						None,
					),
				};
				let response = tg::sandbox::control::ClientResponse { error, id, output };
				sender
					.send(tg::sandbox::control::ClientMessage::Response(response))
					.await?;
			},
		}
		Ok(())
	}
}
