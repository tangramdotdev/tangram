use {
	crate::sandbox::control::local::{Message, Reply},
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

pub(super) struct Control {
	local: tokio::sync::mpsc::Receiver<Message>,
	local_open: bool,
	remote: BoxStream<'static, tg::Result<tg::sandbox::control::ServerMessage>>,
	remote_open: bool,
	sender: super::SandboxControlSender,
}

impl Control {
	#[must_use]
	pub(super) fn new(
		control: crate::control::Stream<
			tg::sandbox::control::ServerMessage,
			tg::sandbox::control::ClientMessage,
		>,
		local: tokio::sync::mpsc::Receiver<Message>,
	) -> Self {
		let sender = control.sender();
		// Retain the receive future across selections because acknowledging a remote request can yield.
		let remote = futures::stream::try_unfold(control, |mut control| async move {
			let message = control.recv_with_ack().await?;
			Ok::<_, tg::Error>(message.map(|message| (message, control)))
		})
		.boxed();
		Self {
			local,
			local_open: true,
			remote,
			remote_open: true,
			sender,
		}
	}

	pub(super) async fn recv(&mut self) -> tg::Result<Option<Message>> {
		loop {
			tokio::select! {
				message = self.local.recv(), if self.local_open => {
					let Some(message) = message else {
						self.local_open = false;
						continue;
					};
					return Ok(Some(message));
				},
				message = self.remote.try_next(), if self.remote_open => {
					let Some(message) = message? else {
						self.remote_open = false;
						return Ok(None);
					};
					match message {
						tg::sandbox::control::ServerMessage::Ack(_) => unreachable!(),
						tg::sandbox::control::ServerMessage::Notification(notification) => match notification {},
						tg::sandbox::control::ServerMessage::Request(request) => {
							let sender = Reply::Remote { id: request.id, sender: self.sender.clone() };
							let message = Message { arg: request.arg, sender };
							return Ok(Some(message));
						},
						tg::sandbox::control::ServerMessage::Response(_) => {},
					}
				},
				else => return futures::future::pending().await,
			}
		}
	}

	#[must_use]
	pub(super) fn sender(&self) -> super::SandboxControlSender {
		self.sender.clone()
	}
}
