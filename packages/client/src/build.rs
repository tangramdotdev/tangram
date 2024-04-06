pub use self::{outcome::Outcome, status::Status};
use crate::{
	self as tg,
	util::http::{empty, full},
};
use bytes::Bytes;
use futures::{
	stream::{self, FuturesUnordered},
	Stream, StreamExt as _, TryStreamExt as _,
};
use http_body_util::BodyExt as _;

pub mod children;
pub mod log;
pub mod outcome;
pub mod status;

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
pub struct Build {
	id: Id,
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
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Retry {
	#[default]
	Canceled,
	Failed,
	Succeeded,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListArg {
	pub limit: Option<u64>,
	pub order: Option<Order>,
	pub status: Option<Status>,
	pub target: Option<tg::target::Id>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub enum Order {
	#[serde(rename = "created_at")]
	CreatedAt,
	#[serde(rename = "created_at.desc")]
	CreatedAtDesc,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ListOutput {
	pub items: Vec<GetOutput>,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct GetArg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOutput {
	pub id: Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcome: Option<outcome::Data>,
	pub retry: Retry,
	pub status: Status,
	pub target: tg::target::Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub weight: Option<u64>,
	#[serde(with = "time::serde::rfc3339")]
	pub created_at: time::OffsetDateTime,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub queued_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub started_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub finished_at: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct PutArg {
	pub id: Id,
	pub children: Vec<Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,
	pub host: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::blob::Id>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outcome: Option<outcome::Data>,
	pub retry: Retry,
	pub status: Status,
	pub target: tg::target::Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub weight: Option<u64>,
	#[serde(with = "time::serde::rfc3339")]
	pub created_at: time::OffsetDateTime,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub queued_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub started_at: Option<time::OffsetDateTime>,
	#[serde(
		default,
		skip_serializing_if = "Option::is_none",
		with = "time::serde::rfc3339::option"
	)]
	pub finished_at: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateArg {
	pub parent: Option<Id>,
	pub remote: bool,
	pub retry: Retry,
	pub target: tg::target::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct GetOrCreateOutput {
	pub id: Id,
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

	pub async fn new(tg: &impl tg::Handle, arg: GetOrCreateArg) -> tg::Result<Self> {
		let output = tg.get_or_create_build(arg).await?;
		let build = Build::with_id(output.id);
		Ok(build)
	}

	pub async fn children(
		&self,
		tg: &impl tg::Handle,
		arg: children::GetArg,
	) -> tg::Result<impl Stream<Item = tg::Result<Self>> + Send + 'static> {
		self.try_get_children(tg, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_children(
		&self,
		tg: &impl tg::Handle,
		arg: children::GetArg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Self>> + Send + 'static>> {
		Ok(tg
			.try_get_build_children(self.id(), arg, None)
			.await?
			.map(|stream| {
				stream
					.map_ok(|chunk| {
						stream::iter(chunk.items.into_iter().map(Build::with_id).map(Ok))
					})
					.try_flatten()
					.boxed()
			}))
	}

	pub async fn add_child(&self, tg: &impl tg::Handle, child: &Self) -> tg::Result<()> {
		let id = self.id();
		let child_id = child.id();
		tg.add_build_child(id, child_id).await?;
		Ok(())
	}

	pub async fn log(
		&self,
		tg: &impl tg::Handle,
		arg: log::GetArg,
	) -> tg::Result<impl Stream<Item = tg::Result<log::Chunk>> + Send + 'static> {
		self.try_get_log(tg, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_log(
		&self,
		tg: &impl tg::Handle,
		arg: log::GetArg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<log::Chunk>> + Send + 'static>> {
		tg.try_get_build_log(self.id(), arg, None)
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}

	pub async fn add_log(&self, tg: &impl tg::Handle, log: Bytes) -> tg::Result<()> {
		let id = self.id();
		tg.add_build_log(id, log).await?;
		Ok(())
	}

	pub async fn outcome(&self, tg: &impl tg::Handle) -> tg::Result<Outcome> {
		self.get_outcome(tg, outcome::GetArg::default())
			.await?
			.ok_or_else(|| tg::error!("failed to get the outcome"))
	}

	pub async fn get_outcome(
		&self,
		tg: &impl tg::Handle,
		arg: outcome::GetArg,
	) -> tg::Result<Option<Outcome>> {
		self.try_get_outcome(tg, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_outcome(
		&self,
		tg: &impl tg::Handle,
		arg: outcome::GetArg,
	) -> tg::Result<Option<Option<Outcome>>> {
		tg.try_get_build_outcome(self.id(), arg, None).await
	}

	pub async fn cancel(&self, tg: &impl tg::Handle) -> tg::Result<()> {
		let id = self.id();
		tg.set_build_outcome(id, Outcome::Canceled).await?;
		Ok(())
	}

	pub async fn set_outcome(&self, tg: &impl tg::Handle, outcome: Outcome) -> tg::Result<()> {
		let id = self.id();
		tg.set_build_outcome(id, outcome).await?;
		Ok(())
	}

	pub async fn retry(&self, tg: &impl tg::Handle) -> tg::Result<Retry> {
		self.try_get_retry(tg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_retry(&self, tg: &impl tg::Handle) -> tg::Result<Option<Retry>> {
		let arg = tg::build::GetArg::default();
		let Some(output) = tg.try_get_build(&self.id, arg).await? else {
			return Ok(None);
		};
		Ok(Some(output.retry))
	}

	pub async fn status(
		&self,
		tg: &impl tg::Handle,
		arg: status::GetArg,
	) -> tg::Result<impl Stream<Item = tg::Result<Status>> + Send + 'static> {
		self.try_get_status(tg, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_status(
		&self,
		tg: &impl tg::Handle,
		arg: status::GetArg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<Status>> + Send + 'static>> {
		tg.try_get_build_status(self.id(), arg, None)
			.await
			.map(|option| option.map(futures::StreamExt::boxed))
	}

	pub async fn target(&self, tg: &impl tg::Handle) -> tg::Result<tg::Target> {
		self.try_get_target(tg)
			.await?
			.ok_or_else(|| tg::error!("failed to get the build"))
	}

	pub async fn try_get_target(&self, tg: &impl tg::Handle) -> tg::Result<Option<tg::Target>> {
		let arg = tg::build::GetArg::default();
		let Some(output) = tg.try_get_build(&self.id, arg).await? else {
			return Ok(None);
		};
		let id = output.target.clone();
		let target = tg::Target::with_id(id);
		Ok(Some(target))
	}

	pub async fn push(&self, tg: &impl tg::Handle, remote: &impl tg::Handle) -> tg::Result<()> {
		let arg = tg::build::GetArg::default();
		let output = tg.get_build(&self.id, arg).await?;
		let arg = tg::build::children::GetArg {
			timeout: Some(std::time::Duration::ZERO),
			..Default::default()
		};
		let children = tg
			.get_build_children(&self.id, arg, None)
			.await?
			.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, tg::Error>))
			.try_flatten()
			.try_collect()
			.await?;
		let arg = PutArg {
			id: output.id,
			children,
			count: output.count,
			host: output.host,
			log: output.log,
			outcome: output.outcome,
			retry: output.retry,
			status: output.status,
			target: output.target,
			weight: output.weight,
			created_at: output.created_at,
			queued_at: output.queued_at,
			started_at: output.started_at,
			finished_at: output.finished_at,
		};
		arg.children
			.iter()
			.cloned()
			.map(Self::with_id)
			.map(|build| async move { build.push(tg, remote).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		arg.objects()
			.iter()
			.cloned()
			.map(tg::object::Handle::with_id)
			.map(|object| async move { object.push(tg, remote, None).await })
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;
		remote
			.put_build(&self.id, &arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		Ok(())
	}

	pub async fn pull(&self, _tg: &impl tg::Handle, _remote: &impl tg::Handle) -> tg::Result<()> {
		Err(tg::error!("unimplemented"))
	}
}

impl GetOutput {
	pub fn objects(&self) -> Vec<tg::object::Id> {
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

impl PutArg {
	pub fn objects(&self) -> Vec<tg::object::Id> {
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
			Self::Canceled => Retry::Canceled,
			Self::Failed(_) => Retry::Failed,
			Self::Succeeded(_) => Retry::Succeeded,
		}
	}

	pub fn into_result(self) -> tg::Result<tg::Value> {
		match self {
			Self::Canceled => Err(tg::error!("the build was canceled")),
			Self::Failed(error) => Err(error),
			Self::Succeeded(value) => Ok(value),
		}
	}

	pub async fn data(&self, tg: &impl tg::Handle) -> tg::Result<outcome::Data> {
		Ok(match self {
			Self::Canceled => outcome::Data::Canceled,
			Self::Failed(error) => outcome::Data::Failed(error.clone()),
			Self::Succeeded(value) => outcome::Data::Succeeded(value.data(tg, None).await?),
		})
	}
}

impl tg::Client {
	pub async fn list_builds(&self, arg: tg::build::ListArg) -> tg::Result<tg::build::ListOutput> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the search params"))?;
		let uri = format!("/builds?{search_params}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = empty();
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		Ok(output)
	}

	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::GetArg,
	) -> tg::Result<Option<tg::build::GetOutput>> {
		let method = http::Method::GET;
		let search_params = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the search params"))?;
		let uri = format!("/builds/{id}?{search_params}");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(Some(output))
	}

	pub async fn put_build(&self, id: &tg::build::Id, arg: &tg::build::PutArg) -> tg::Result<()> {
		let method = http::Method::PUT;
		let uri = format!("/builds/{id}");
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.inner.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the data"))?;
		let body = full(json);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}

	pub async fn push_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/push");
		let body = empty();
		let mut request = http::request::Builder::default().method(method).uri(uri);
		if let Some(token) = self.inner.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}

	pub async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = format!("/builds/{id}/pull");
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		Ok(())
	}

	pub async fn get_or_create_build(
		&self,
		arg: tg::build::GetOrCreateArg,
	) -> tg::Result<tg::build::GetOrCreateOutput> {
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
		if let Some(token) = self.inner.token.as_ref() {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let json = serde_json::to_vec(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(json);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("the request did not succeed"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))?;
		Ok(output)
	}
}

impl TryFrom<outcome::Data> for Outcome {
	type Error = tg::Error;

	fn try_from(data: outcome::Data) -> tg::Result<Self, Self::Error> {
		match data {
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

impl std::fmt::Display for Retry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Canceled => write!(f, "canceled"),
			Self::Failed => write!(f, "failed"),
			Self::Succeeded => write!(f, "succeeded"),
		}
	}
}

impl std::str::FromStr for Retry {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		match s {
			"canceled" => Ok(Self::Canceled),
			"failed" => Ok(Self::Failed),
			"succeeded" => Ok(Self::Succeeded),
			retry => Err(tg::error!(%retry, "invalid value")),
		}
	}
}
