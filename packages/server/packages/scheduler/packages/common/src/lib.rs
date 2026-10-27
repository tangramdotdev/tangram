use {
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_server_scheduler_core::{RequestArg, ResponseOutput},
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Message {
	Ack(Ack),
	Notification(Notification),
	Request(Request),
	Response(Response),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Ack {
	pub id: String,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Notification {
	BorrowableCapacity(BorrowableCapacityNotification),
	Heartbeat(HeartbeatNotification),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BorrowableCapacityNotification {
	pub available: bool,
	pub capacity: tg::runner::Capacity,
	pub heartbeat_index: u64,
	pub parent: tg::sandbox::Id,
	pub runner: tg::runner::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
	pub arg: RequestArg,
	pub id: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
	pub error: Option<tg::error::Data>,
	pub id: String,
	pub output: Option<ResponseOutput>,
	pub scheduler: tg::scheduler::Id,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatAcknowledgement {
	pub connection_index: u64,
	pub heartbeat_index: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HeartbeatNotification {
	pub capacity: tg::runner::control::Capacity,
	pub connection_index: u64,
	pub heartbeat_index: u64,
	pub runner: tg::runner::Id,
	pub sandboxes: BTreeSet<tg::sandbox::Id>,
}

#[must_use]
pub fn runner_heartbeat_subject(scheduler: &tg::scheduler::Id, runner: &tg::runner::Id) -> String {
	format!("schedulers.{scheduler}.runners.{runner}.heartbeat")
}

#[must_use]
pub fn scheduler_heartbeat_subject(scheduler: &tg::scheduler::Id) -> String {
	format!("schedulers.{scheduler}.heartbeat")
}

#[must_use]
pub fn scheduler_server_subject(scheduler: Option<&tg::scheduler::Id>) -> String {
	scheduler.map_or_else(
		|| "schedulers.server".to_owned(),
		|scheduler| format!("schedulers.{scheduler}.server"),
	)
}

impl tangram_messenger::Payload for Message {
	fn deserialize(bytes: bytes::Bytes) -> Result<Self, tangram_messenger::Error>
	where
		Self: Sized,
	{
		let message =
			serde_json::from_slice(&bytes).map_err(tangram_messenger::Error::deserialization)?;
		Ok(message)
	}

	fn serialize(&self) -> Result<bytes::Bytes, tangram_messenger::Error> {
		let message = serde_json::to_vec(self).map_err(tangram_messenger::Error::serialization)?;
		Ok(message.into())
	}
}
