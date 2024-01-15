pub use self::outcome::Outcome;
use crate::{
	blob, id, object, target, Error, Handle, Result, System, Target, User, Value, WrapErr,
};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use futures::{
	stream::{BoxStream, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use itertools::Itertools;
use tangram_error::error;

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

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct State {
	pub children: Vec<Id>,
	pub id: Id,
	pub log: Option<blob::Id>,
	pub outcome: Option<outcome::Data>,
	pub status: Status,
	pub target: target::Id,
	#[serde(with = "time::serde::rfc3339")]
	pub timestamp: time::OffsetDateTime,
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct GetLogArg {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub pos: Option<u64>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub len: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct LogEntry {
	pub pos: u64,
	pub bytes: Bytes,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Status {
	Queued,
	Running,
	Finished,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Options {
	pub depth: u64,
	pub parent: Option<Id>,
	pub remote: bool,
	pub retry: Retry,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListArg {
	pub limit: u64,
	pub sort: ListSort,
	pub target: target::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ListSort {
	Timestamp,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListOutput {
	pub values: Vec<State>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub state: State,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct PutOutput {
	pub missing: Missing,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Missing {
	pub builds: Vec<Id>,
	pub objects: Vec<object::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateArg {
	pub target: target::Id,
	pub options: Options,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateOutput {
	pub id: Id,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct GetBuildFromQueueArg {
	#[serde(
		deserialize_with = "deserialize_try_get_queue_item_arg_hosts",
		serialize_with = "serialize_try_get_queue_item_arg_hosts"
	)]
	pub hosts: Option<Vec<System>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetBuildFromQueueOutput {
	pub build: Id,
	pub options: Options,
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
		let id = target.id(tg).await?;
		let arg = GetOrCreateArg {
			target: id.clone(),
			options,
		};
		let output = tg.get_or_create_build(None, arg).await?;
		let build = Build::with_id(output.id);
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

	pub async fn log(
		&self,
		tg: &dyn Handle,
		arg: GetLogArg,
	) -> Result<BoxStream<'static, Result<LogEntry>>> {
		self.try_get_log(tg, arg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_log(
		&self,
		tg: &dyn Handle,
		arg: GetLogArg,
	) -> Result<Option<BoxStream<'static, Result<LogEntry>>>> {
		tg.try_get_build_log(self.id(), arg).await
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
		let GetOutput { state } = tg.get_build(&self.id).await?;
		let output = remote
			.try_put_build(user, &self.id, &state)
			.await
			.wrap_err("Failed to put the object.")?;
		output
			.missing
			.builds
			.iter()
			.cloned()
			.map(Self::with_id)
			.map(|build| async move { build.push(user, tg, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		output
			.missing
			.objects
			.iter()
			.cloned()
			.map(object::Handle::with_id)
			.map(|object| async move { object.push(tg, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		if !output.missing.builds.is_empty() || !output.missing.objects.is_empty() {
			let output = remote
				.try_put_build(user, &self.id, &state)
				.await
				.wrap_err("Failed to put the build.")?;
			if !output.missing.builds.is_empty() || !output.missing.objects.is_empty() {
				return Err(error!("Expected all children to be stored."));
			}
		}
		Ok(())
	}
}

impl State {
	pub fn objects(&self) -> Vec<object::Id> {
		let log = self.log.iter().map(|id| id.clone().into());
		let outcome = self
			.outcome
			.as_ref()
			.map(|outcome| {
				if let outcome::Data::Succeeded(value) = outcome {
					value.children()
				} else {
					vec![]
				}
			})
			.into_iter()
			.flatten();
		let target = std::iter::once(self.target.clone().into());
		log.chain(outcome).chain(target).collect()
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
			return Err(error!("Invalid kind."));
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
			_ => Err(error!("Invalid value.")),
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
			_ => Err(error!("Invalid value.")),
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

fn serialize_try_get_queue_item_arg_hosts<S>(
	value: &Option<Vec<System>>,
	serializer: S,
) -> Result<S::Ok, S::Error>
where
	S: serde::Serializer,
{
	match value {
		Some(hosts) => serializer.serialize_str(&hosts.iter().join(",")),
		None => serializer.serialize_unit(),
	}
}

fn deserialize_try_get_queue_item_arg_hosts<'de, D>(
	deserializer: D,
) -> Result<Option<Vec<System>>, D::Error>
where
	D: serde::Deserializer<'de>,
{
	struct Visitor;

	impl<'de> serde::de::Visitor<'de> for Visitor {
		type Value = Option<Vec<System>>;

		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("a string with comma-separated values or null")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			let values = value
				.split(',')
				.map(std::str::FromStr::from_str)
				.try_collect()
				.map_err(|_| serde::de::Error::custom("invalid system"))?;
			Ok(Some(values))
		}

		fn visit_none<E>(self) -> Result<Self::Value, E>
		where
			E: serde::de::Error,
		{
			Ok(None)
		}
	}

	deserializer.deserialize_any(Visitor)
}
