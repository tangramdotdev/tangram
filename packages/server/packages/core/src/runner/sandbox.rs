use {super::process::ConnectedEvent, tangram_client::prelude::*};

pub struct SpawnSandboxTaskArg {
	pub allocation: crate::runner::capacity::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub creator: Option<tg::Principal>,
	pub id: Option<tg::sandbox::Id>,
	pub location: tg::Location,
	pub process: Option<tg::runner::control::Process>,
	pub token: Option<String>,
}

#[must_use]
pub struct SpawnSandboxTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

pub enum Event {
	Destroyed,
	Ready(ReadyEvent),
}

#[derive(Clone, Debug)]
pub struct ReadyEvent {
	pub connected_event: Option<ConnectedEvent>,
	pub sandbox: tg::sandbox::Id,
}
