use crate::{self as tg, handle::Ext as _};
use std::pin::pin;
use tangram_futures::stream::Ext;

pub use self::status::Status;

pub mod children;
pub mod dequeue;
pub mod finish;
pub mod get;
pub mod heartbeat;
pub mod log;
pub mod pull;
pub mod push;
pub mod put;
pub mod retry;
pub mod start;
pub mod status;
pub mod touch;

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

#[derive(Clone, Debug)]
pub struct Process {
	id: Id,
	token: Option<String>,
	remote: Option<String>,
}

impl Id {
	#[allow(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(crate::Id::new_uuidv7(tg::id::Kind::Process))
	}
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
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_command<H>(&self, handle: &H) -> tg::Result<Option<tg::Command>>
	where
		H: tg::Handle,
	{
		let Some(build) = handle.try_get_process(&self.id).await? else {
			return Ok(None);
		};
		let id = build.command.clone();
		let target = tg::Command::with_id(id);
		Ok(Some(target))
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		let Some(stream) = handle.try_get_process_status(&self.id).await? else {
			return Err(tg::error!("failed to get the build status stream"));
		};
		let Some(Ok(_)) = pin!(stream).last().await else {
			return Err(tg::error!("failed to get the last build status"));
		};
		let output = handle.get_process(&self.id).await?;
		let output = match output.status {
			Status::Canceled | Status::Failed => {
				let error = output
					.error
					.ok_or_else(|| tg::error!("expected the error to be set"))?;
				return Err(error);
			},
			Status::Succeeded => output
				.output
				.ok_or_else(|| tg::error!("expected the output to be set"))?
				.try_into()?,
			_ => {
				return Err(tg::error!("expected the build to be finished"));
			},
		};
		Ok(output)
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = tg::Error;

	fn try_from(value: crate::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Process {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}
