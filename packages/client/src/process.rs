use crate::{self as tg, handle::Ext as _};
use futures::TryFutureExt as _;
use std::pin::pin;
use tangram_futures::stream::TryExt as _;

pub use self::{id::Id, status::Status};

pub mod children;
pub mod dequeue;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod id;
pub mod input;
pub mod log;
pub mod pull;
pub mod push;
pub mod put;
pub mod retry;
pub mod signal;
pub mod start;
pub mod status;
pub mod touch;
pub mod wait;

#[derive(Clone, Debug)]
pub struct Process {
	id: Id,
	token: Option<String>,
	remote: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Sandbox {
	pub filesystem: bool,
	pub network: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Exit {
	Code { code: i32 },
	Signal { signal: i32 },
}

impl Process {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self {
			id,
			remote: None,
			token: None,
		}
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.id
	}

	#[must_use]
	pub fn remote(&self) -> Option<&str> {
		self.remote.as_deref()
	}

	#[must_use]
	pub fn token(&self) -> Option<&str> {
		self.token.as_deref()
	}

	pub async fn command<H>(&self, handle: &H) -> tg::Result<tg::Command>
	where
		H: tg::Handle,
	{
		self.try_get_command(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_command<H>(&self, handle: &H) -> tg::Result<Option<tg::Command>>
	where
		H: tg::Handle,
	{
		let Some(process) = handle.try_get_process(&self.id).await? else {
			return Ok(None);
		};
		let id = process.command.clone();
		let command = tg::Command::with_id(id);
		Ok(Some(command))
	}

	pub async fn wait<H>(&self, handle: &H) -> tg::Result<tg::process::wait::Output>
	where
		H: tg::Handle,
	{
		let stream = handle.get_process_wait(&self.id).await?;
		let Some(tg::process::wait::Event::Output(output)) =
			pin!(stream).try_last().await?
		else {
			return Err(tg::error!("failed to get the last process event"));
		};
		let output = tg::process::wait::Output {
			error: output.error,
			exit: output.exit,
			output: output.output.map(tg::Value::try_from).transpose()?,
			status: output.status,
		};
		Ok(output)
	}

	pub async fn exit<H>(&self, handle: &H) -> tg::Result<Option<tg::process::Exit>>
	where
		H: tg::Handle,
	{
		self.wait(handle).map_ok(|output| output.exit).await
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<Option<tg::Value>>
	where
		H: tg::Handle,
	{
		self.wait(handle).map_ok(|output| output.output).await
	}

	pub async fn error<H>(&self, handle: &H) -> tg::Result<Option<tg::Error>>
	where
		H: tg::Handle,
	{
		self.wait(handle).map_ok(|output| output.error).await
	}
}
