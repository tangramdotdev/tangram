use {
	futures::FutureExt as _, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_server_control as control,
};

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

/// A response to a request, which is the request's output or a reason to send the request again.
pub type Response = ControlFlow<tg::sandbox::control::ClientResponseOutput, tg::control::Retry>;

pub enum Reply {
	Local(tokio::sync::oneshot::Sender<tg::Result<Response>>),
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
	) -> tg::Result<Response> {
		self.send_request(arg).await?.await?
	}

	pub async fn send_request(
		&self,
		arg: tg::sandbox::control::ServerRequestArg,
	) -> tg::Result<futures::future::BoxFuture<'static, tg::Result<tg::Result<Response>>>> {
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
	/// Send the output of a request, or an error.
	pub async fn send(
		self,
		result: tg::Result<tg::sandbox::control::ClientResponseOutput>,
	) -> tg::Result<()> {
		self.send_response(result.map(ControlFlow::Break)).await
	}

	/// Ask the sender of a request to send the request again.
	pub async fn send_retry(self, retry: tg::control::Retry) -> tg::Result<()> {
		self.send_response(Ok(ControlFlow::Continue(retry))).await
	}

	async fn send_response(self, result: tg::Result<Response>) -> tg::Result<()> {
		match self {
			Self::Local(sender) => {
				// An abandoned caller must not stop the sandbox.
				sender.send(result).ok();
			},
			Self::Remote { id, sender } => {
				let response = match result {
					Ok(ControlFlow::Break(output)) => {
						tg::sandbox::control::ClientResponse::with_output(id, output)
					},
					Ok(ControlFlow::Continue(retry)) => {
						tg::sandbox::control::ClientResponse::with_retry(id, retry)
					},
					Err(error) => {
						let error = tg::error::Data {
							message: Some(error.to_string()),
							..Default::default()
						};
						tg::sandbox::control::ClientResponse::with_error(id, error)
					},
				};
				sender
					.send(tg::sandbox::control::ClientMessage::Response(response))
					.await?;
			},
		}
		Ok(())
	}
}
