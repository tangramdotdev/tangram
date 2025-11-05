use tangram_client::prelude::*;

mod close;
mod create;
mod delete;
mod read;
mod write;

pub(crate) struct Pipe {
	pub(crate) receiver: tokio::net::unix::pipe::Receiver,
	pub(crate) sender: Option<tokio::net::unix::pipe::Sender>,
}

impl Pipe {
	async fn new() -> tg::Result<Self> {
		let (sender, receiver) = tokio::net::unix::pipe::pipe()
			.map_err(|source| tg::error!(!source, "failed to create pipes"))?;
		Ok(Self {
			receiver,
			sender: Some(sender),
		})
	}
}
