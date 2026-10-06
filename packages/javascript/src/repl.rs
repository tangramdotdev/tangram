use tangram_client::prelude::*;

pub type Receiver = tokio::sync::mpsc::UnboundedReceiver<Command>;

pub type Sender = tokio::sync::mpsc::UnboundedSender<Command>;

pub struct Command {
	pub source: String,
	pub response: tokio::sync::oneshot::Sender<tg::Result<()>>,
}
