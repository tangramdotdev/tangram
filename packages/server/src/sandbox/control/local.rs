use {crate::control, tangram_client::prelude::*};

#[derive(Clone)]
pub(crate) struct Local {
	sender: tokio::sync::mpsc::Sender<Message>,
}

pub(crate) struct Message {
	pub arg: tg::sandbox::control::ServerRequestArg,
	pub sender: Reply,
}

pub(crate) enum Reply {
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
	pub(crate) fn new() -> (Self, tokio::sync::mpsc::Receiver<Message>) {
		let (sender, receiver) = tokio::sync::mpsc::channel(256);
		(Self { sender }, receiver)
	}

	#[must_use]
	pub(crate) fn is_closed(&self) -> bool {
		self.sender.is_closed()
	}

	pub(crate) async fn request(
		&self,
		arg: tg::sandbox::control::ServerRequestArg,
	) -> tg::Result<tg::Result<tg::sandbox::control::ClientResponseOutput>> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let message = Message {
			arg,
			sender: Reply::Local(sender),
		};
		self.sender
			.send(message)
			.await
			.map_err(|_| tg::error!("the runner sandbox control channel closed"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the runner sandbox control response channel closed"))
	}
}

impl Reply {
	pub(crate) async fn send(
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
