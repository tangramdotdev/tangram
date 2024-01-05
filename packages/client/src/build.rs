pub use self::outcome::Outcome;
use crate::{blob, id, object, target, Error, Handle, Result, Target, User, Value, WrapErr};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use futures::{
	stream::{BoxStream, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use tangram_error::{error, return_error};

#[derive(
	Clone,
	Debug,
	Display,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "crate::Id", try_from = "crate::Id")]
pub struct Id(crate::Id);

#[derive(Clone, Debug)]
pub struct Build {
	id: Id,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Status {
	Queued,
	Running,
	Finished,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub children: Vec<Id>,
	pub log: Option<blob::Id>,
	pub outcome: Option<outcome::Data>,
	pub status: Status,
	pub target: target::Id,
}

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub depth: u64,
	pub parent: Option<Build>,
	pub remote: bool,
	pub retry: Retry,
	pub user: Option<User>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct PutOutput {
	pub missing: Missing,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Missing {
	pub children: Vec<Id>,
	pub log: bool,
	pub outcome: bool,
	pub target: bool,
}

pub mod outcome {
	use crate::{value, Value};
	use derive_more::TryUnwrap;
	use tangram_error::Error;

	#[derive(Clone, Debug, serde::Deserialize, TryUnwrap)]
	#[serde(try_from = "Data")]
	#[try_unwrap(ref)]
	pub enum Outcome {
		Terminated,
		Canceled,
		Failed(Error),
		Succeeded(Value),
	}

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize, TryUnwrap)]
	#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
	#[try_unwrap(ref)]
	pub enum Data {
		Terminated,
		Canceled,
		Failed(Error),
		Succeeded(value::Data),
	}
}

#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(into = "String", try_from = "String")]
pub enum Retry {
	Terminated,
	#[default]
	Canceled,
	Failed,
	Succeeded,
}

impl Id {
	#[allow(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(crate::Id::new_uuidv7(id::Kind::Build))
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
}

impl Build {
	pub async fn new(tg: &dyn Handle, target: Target, options: Options) -> Result<Self> {
		let target_id = target.id(tg).await?;
		let build_id = tg.get_or_create_build(target_id, options).await?;
		let build = Build::with_id(build_id);
		Ok(build)
	}

	pub async fn status(&self, tg: &dyn Handle) -> Result<Status> {
		self.try_get_status(tg)
			.await?
			.wrap_err("Failed to get the status.")
	}

	pub async fn try_get_status(&self, tg: &dyn Handle) -> Result<Option<Status>> {
		tg.try_get_build_status(self.id()).await
	}

	pub async fn target(&self, tg: &dyn Handle) -> Result<Target> {
		self.try_get_target(tg)
			.await?
			.wrap_err("Failed to get the target.")
	}

	pub async fn try_get_target(&self, tg: &dyn Handle) -> Result<Option<Target>> {
		Ok(tg
			.try_get_build_target(self.id())
			.await?
			.map(Target::with_id))
	}

	pub async fn children(&self, tg: &dyn Handle) -> Result<BoxStream<'static, Result<Self>>> {
		self.try_get_children(tg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_children(
		&self,
		tg: &dyn Handle,
	) -> Result<Option<BoxStream<'static, Result<Self>>>> {
		Ok(tg
			.try_get_build_children(self.id())
			.await?
			.map(|children| children.map_ok(Build::with_id).boxed()))
	}

	pub async fn add_child(&self, tg: &dyn Handle, child: &Self) -> Result<()> {
		let id = self.id();
		let child_id = child.id();
		tg.add_build_child(None, id, child_id).await?;
		Ok(())
	}

	pub async fn log(&self, tg: &dyn Handle) -> Result<BoxStream<'static, Result<Bytes>>> {
		self.try_get_log(tg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_log(
		&self,
		tg: &dyn Handle,
	) -> Result<Option<BoxStream<'static, Result<Bytes>>>> {
		tg.try_get_build_log(self.id()).await
	}

	pub async fn add_log(&self, tg: &dyn Handle, log: Bytes) -> Result<()> {
		let id = self.id();
		tg.add_build_log(None, id, log).await?;
		Ok(())
	}

	pub async fn outcome(&self, tg: &dyn Handle) -> Result<Outcome> {
		self.try_get_outcome(tg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_outcome(&self, tg: &dyn Handle) -> Result<Option<Outcome>> {
		tg.try_get_build_outcome(self.id()).await
	}

	pub async fn cancel(&self, tg: &dyn Handle) -> Result<()> {
		let id = self.id();
		tg.finish_build(None, id, Outcome::Canceled).await?;
		Ok(())
	}

	pub async fn finish(
		&self,
		tg: &dyn Handle,
		user: Option<&User>,
		outcome: Outcome,
	) -> Result<()> {
		let id = self.id();
		tg.finish_build(user, id, outcome).await?;
		Ok(())
	}

	#[async_recursion]
	pub async fn push(
		&self,
		user: Option<&'async_recursion User>,
		tg: &dyn Handle,
		remote: &dyn Handle,
	) -> Result<()> {
		let data = tg.get_build(&self.id).await?;
		let output = remote
			.try_put_build(user, &self.id, &data)
			.await
			.wrap_err("Failed to put the object.")?;
		output
			.missing
			.children
			.iter()
			.cloned()
			.map(Self::with_id)
			.map(|build| async move { build.push(user, tg, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		if output.missing.log {
			if let Some(log) = data.log.clone() {
				object::Handle::with_id(log.into()).push(tg, remote).await?;
			}
		}
		if output.missing.outcome {
			if let Some(outcome::Data::Succeeded(outcome)) = data.outcome.clone() {
				Value::try_from(outcome)?.push(tg, remote).await?;
			}
		}
		if output.missing.target {
			object::Handle::with_id(data.target.clone().into())
				.push(tg, remote)
				.await?;
		}
		if !output.missing.children.is_empty()
			|| output.missing.log
			|| output.missing.outcome
			|| output.missing.target
		{
			let output = remote
				.try_put_build(user, &self.id, &data)
				.await
				.wrap_err("Failed to put the build.")?;
			if !output.missing.children.is_empty()
				|| output.missing.log
				|| output.missing.outcome
				|| output.missing.target
			{
				return Err(error!("Expected all children to be stored."));
			}
		}
		Ok(())
	}
}

impl Outcome {
	#[must_use]
	pub fn retry(&self) -> Retry {
		match self {
			Self::Terminated => Retry::Terminated,
			Self::Canceled => Retry::Canceled,
			Self::Failed(_) => Retry::Failed,
			Self::Succeeded(_) => Retry::Succeeded,
		}
	}

	pub fn into_result(self) -> Result<Value> {
		match self {
			Self::Terminated => Err(error!("The build was terminated.")),
			Self::Canceled => Err(error!("The build was canceled.")),
			Self::Failed(error) => Err(error),
			Self::Succeeded(value) => Ok(value),
		}
	}

	pub async fn data(&self, tg: &dyn Handle) -> Result<outcome::Data> {
		Ok(match self {
			Self::Terminated => outcome::Data::Terminated,
			Self::Canceled => outcome::Data::Canceled,
			Self::Failed(error) => outcome::Data::Failed(error.clone()),
			Self::Succeeded(value) => outcome::Data::Succeeded(value.data(tg).await?),
		})
	}
}

impl TryFrom<outcome::Data> for Outcome {
	type Error = Error;

	fn try_from(data: outcome::Data) -> Result<Self, Self::Error> {
		match data {
			outcome::Data::Terminated => Ok(Outcome::Terminated),
			outcome::Data::Canceled => Ok(Outcome::Canceled),
			outcome::Data::Failed(error) => Ok(Outcome::Failed(error)),
			outcome::Data::Succeeded(value) => Ok(Outcome::Succeeded(value.try_into()?)),
		}
	}
}

impl From<Id> for crate::Id {
	fn from(value: Id) -> Self {
		value.0
	}
}

impl TryFrom<crate::Id> for Id {
	type Error = Error;

	fn try_from(value: crate::Id) -> Result<Self, Self::Error> {
		if value.kind() != id::Kind::Build {
			return_error!("Invalid kind.");
		}
		Ok(Self(value))
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		crate::Id::from_str(s)?.try_into()
	}
}

impl std::fmt::Display for Status {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Queued => write!(f, "queued"),
			Self::Running => write!(f, "running"),
			Self::Finished => write!(f, "finished"),
		}
	}
}

impl std::str::FromStr for Status {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"queued" => Ok(Self::Queued),
			"running" => Ok(Self::Running),
			"finished" => Ok(Self::Finished),
			_ => return_error!("Invalid value."),
		}
	}
}

impl From<Status> for String {
	fn from(value: Status) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Status {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

impl std::fmt::Display for Retry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Terminated => write!(f, "terminated"),
			Self::Canceled => write!(f, "canceled"),
			Self::Failed => write!(f, "failed"),
			Self::Succeeded => write!(f, "succeeded"),
		}
	}
}

impl std::str::FromStr for Retry {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"terminated" => Ok(Self::Terminated),
			"canceled" => Ok(Self::Canceled),
			"failed" => Ok(Self::Failed),
			"succeeded" => Ok(Self::Succeeded),
			_ => return_error!("Invalid value."),
		}
	}
}

impl From<Retry> for String {
	fn from(value: Retry) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Retry {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}

pub mod queue {
	use super::{Id, Retry};
	use crate::System;

	#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
	pub struct Item {
		pub build: Id,
		pub host: System,
		pub depth: u64,
		pub retry: Retry,
	}

	impl PartialOrd for Item {
		fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
			Some(self.depth.cmp(&other.depth))
		}
	}

	impl Ord for Item {
		fn cmp(&self, other: &Self) -> std::cmp::Ordering {
			self.depth.cmp(&other.depth)
		}
	}

	impl PartialEq for Item {
		fn eq(&self, other: &Self) -> bool {
			self.depth == other.depth
		}
	}

	impl Eq for Item {}
}
