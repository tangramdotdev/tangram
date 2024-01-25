pub use self::{outcome::Outcome, status::Status};
use crate as tg;
use crate::{blob, id, object, target, Client, Handle, Target, User, Value, WrapErr};
use async_recursion::async_recursion;
use bytes::Bytes;
use derive_more::Display;
use futures::{
	stream::{self, BoxStream, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use http_body_util::BodyExt;
use std::sync::Arc;
use tangram_error::{error, Error, Result};
use tangram_util::http::{empty, full};

pub mod children;
pub mod log;
pub mod outcome;
pub mod queue;
pub mod status;

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
	state: Arc<std::sync::RwLock<Option<Arc<State>>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct State {
	/// The build's children.
	#[serde(default)]
	pub children: Vec<Id>,

	/// The build's ID.
	pub id: Id,

	/// The build's log.
	#[serde(default)]
	pub log: Option<blob::Id>,

	/// The build's outcome.
	#[serde(default)]
	pub outcome: Option<outcome::Data>,

	/// The build's status.
	pub status: Status,

	/// The build's target.
	pub target: target::Id,

	/// The build's timestamp.
	#[serde(with = "time::serde::rfc3339")]
	pub timestamp: time::OffsetDateTime,
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
		let state = Arc::new(std::sync::RwLock::new(None));
		Self { id, state }
	}

	#[must_use]
	pub fn id(&self) -> &Id {
		&self.id
	}

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

	pub async fn load(&self, tg: &dyn Handle) -> Result<()> {
		let state = tg.get_build(self.id()).await?.state;
		let state = Arc::new(state);
		self.state.write().unwrap().replace(state);
		Ok(())
	}

	pub async fn state(&self, tg: &dyn Handle) -> Result<Arc<State>> {
		let state = self.state.read().unwrap().clone();
		if let Some(state) = state {
			return Ok(state);
		}
		self.load(tg).await?;
		Ok(self.state.read().unwrap().clone().unwrap())
	}

	pub async fn status(
		&self,
		tg: &dyn Handle,
		arg: status::GetArg,
	) -> Result<BoxStream<'static, Result<Status>>> {
		self.try_get_status(tg, arg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_status(
		&self,
		tg: &dyn Handle,
		arg: status::GetArg,
	) -> Result<Option<BoxStream<'static, Result<Status>>>> {
		tg.try_get_build_status(self.id(), arg).await
	}

	pub async fn target(&self, tg: &dyn Handle) -> Result<Target> {
		let state = self.state(tg).await?;
		let id = state.target.clone();
		let target = Target::with_id(id);
		Ok(target)
	}

	pub async fn children(
		&self,
		tg: &dyn Handle,
		arg: children::GetArg,
	) -> Result<BoxStream<'static, Result<Self>>> {
		self.try_get_children(tg, arg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_children(
		&self,
		tg: &dyn Handle,
		arg: children::GetArg,
	) -> Result<Option<BoxStream<'static, Result<Self>>>> {
		Ok(tg
			.try_get_build_children(self.id(), arg)
			.await?
			.map(|stream| {
				stream
					.map_ok(|chunk| {
						stream::iter(chunk.data.into_iter().map(Build::with_id).map(Ok))
					})
					.try_flatten()
					.boxed()
			}))
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
		arg: log::GetArg,
	) -> Result<BoxStream<'static, Result<log::Chunk>>> {
		self.try_get_log(tg, arg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_log(
		&self,
		tg: &dyn Handle,
		arg: log::GetArg,
	) -> Result<Option<BoxStream<'static, Result<log::Chunk>>>> {
		tg.try_get_build_log(self.id(), arg).await
	}

	pub async fn add_log(&self, tg: &dyn Handle, log: Bytes) -> Result<()> {
		let id = self.id();
		tg.add_build_log(None, id, log).await?;
		Ok(())
	}

	pub async fn outcome(&self, tg: &dyn Handle) -> Result<Outcome> {
		self.get_outcome(tg, outcome::GetArg::default())
			.await?
			.wrap_err("Failed to get the outcome.")
	}

	pub async fn get_outcome(
		&self,
		tg: &dyn Handle,
		arg: outcome::GetArg,
	) -> Result<Option<Outcome>> {
		self.try_get_outcome(tg, arg)
			.await?
			.wrap_err("Failed to get the build.")
	}

	pub async fn try_get_outcome(
		&self,
		tg: &dyn Handle,
		arg: outcome::GetArg,
	) -> Result<Option<Option<Outcome>>> {
		tg.try_get_build_outcome(self.id(), arg).await
	}

	pub async fn cancel(&self, tg: &dyn Handle) -> Result<()> {
		let id = self.id();
		tg.set_build_outcome(None, id, Outcome::Canceled).await?;
		Ok(())
	}

	pub async fn set_outcome(
		&self,
		tg: &dyn Handle,
		user: Option<&User>,
		outcome: Outcome,
	) -> Result<()> {
		let id = self.id();
		tg.set_build_outcome(user, id, outcome).await?;
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

impl Client {
	pub async fn try_list_builds(&self, arg: tg::build::ListArg) -> Result<tg::build::ListOutput> {
		let method = http::Method::GET;
		let search_params =
			serde_urlencoded::to_string(&arg).wrap_err("Failed to serialize the search params.")?;
		let uri = format!("/builds?{search_params}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(output)
	}

	pub async fn get_build_exists(&self, id: &tg::build::Id) -> Result<bool> {
		let method = http::Method::HEAD;
		let uri = format!("/builds/{id}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(false);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		Ok(true)
	}

	pub async fn try_get_build(&self, id: &tg::build::Id) -> Result<Option<tg::build::GetOutput>> {
		let method = http::Method::GET;
		let uri = format!("/builds/{id}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let state = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		let output = tg::build::GetOutput { state };
		Ok(Some(output))
	}

	pub async fn try_put_build(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		state: &tg::build::State,
	) -> Result<tg::build::PutOutput> {
		let method = http::Method::PUT;
		let uri = format!("/builds/{id}");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_string(&state).wrap_err("Failed to serialize the data.")?;
		let body = full(json);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(output)
	}

	pub async fn get_or_create_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::GetOrCreateArg,
	) -> Result<tg::build::GetOrCreateOutput> {
		let method = http::Method::POST;
		let uri = "/builds";
		let mut request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			);
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&arg).wrap_err("Failed to serialize the body.")?;
		let body = full(body);
		let request = request
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let output = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(output)
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
