use crate::{self as tg, handle::Ext as _};
use std::pin::pin;
use tangram_futures::stream::Ext;

pub use self::{retry::Retry, status::Status};

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

#[derive(Clone, Debug, derive_more::Display)]
pub struct Build {
	id: Id,
}

impl Id {
	#[allow(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(crate::Id::new_uuidv7(tg::id::Kind::Build))
	}
}

impl Build {
	#[must_use]
	pub fn with_id(id: Id) -> Self {
		Self { id }
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.id
	}

	pub async fn target<H>(&self, handle: &H) -> tg::Result<tg::Target>
	where
		H: tg::Handle,
	{
		self.try_get_target(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_target<H>(&self, handle: &H) -> tg::Result<Option<tg::Target>>
	where
		H: tg::Handle,
	{
		let Some(build) = handle.try_get_build(&self.id).await? else {
			return Ok(None);
		};
		let id = build.target.clone();
		let target = tg::Target::with_id(id);
		Ok(Some(target))
	}

	pub async fn try_get_output<H>(&self, handle: &H) -> tg::Result<Option<tg::Value>>
	where
		H: tg::Handle,
	{
		let Some(stream) = handle.try_get_build_status(&self.id).await? else {
			return Ok(None);
		};
		let Some(Ok(status)) = pin!(stream).last().await else {
			return Err(tg::error!("failed to get the last build status"));
		};
		if status.is_finished() {
			let build = handle.get_build(&self.id).await?;
			let Some(output) = build.output else {
				return Ok(None);
			};
			let Ok(value) = output.try_into() else {
				return Ok(None);
			};
			Ok(Some(value))
		} else {
			Err(tg::error!("checksum build failed"))
		}
	}

	pub async fn output<H>(&self, handle: &H) -> tg::Result<tg::Value>
	where
		H: tg::Handle,
	{
		self.try_get_output(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build output"))
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
		if value.kind() != tg::id::Kind::Build {
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
