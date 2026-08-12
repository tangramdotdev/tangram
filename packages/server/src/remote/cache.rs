use {
	crate::Session,
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Request {
	Get(GetRequest),
	GroupGet(GroupGetRequest),
	List(ListRequest),
	Match(MatchRequest),
	OrganizationGet(OrganizationGetRequest),
	Resolve(ResolveRequest),
	SandboxGet(SandboxGetRequest),
	TagGet(TagGetRequest),
	UserGet(UserGetRequest),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Response {
	Get(GetResponse),
	GroupGet(GroupGetResponse),
	List(ListResponse),
	Match(MatchResponse),
	OrganizationGet(OrganizationGetResponse),
	Resolve(ResolveResponse),
	SandboxGet(SandboxGetResponse),
	TagGet(TagGetResponse),
	UserGet(UserGetResponse),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct GetRequest {
	pub arg: tg::get::Arg,
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct GetResponse {
	pub output: Option<tg::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct GroupGetRequest {
	pub arg: tg::group::get::Arg,
	pub id: tg::group::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct GroupGetResponse {
	pub output: Option<tg::group::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ListRequest {
	pub arg: tg::list::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ListResponse {
	pub output: tg::list::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct MatchRequest {
	pub arg: tg::match_::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct MatchResponse {
	pub output: tg::match_::Output,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct OrganizationGetRequest {
	pub arg: tg::organization::get::Arg,
	pub id: tg::organization::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct OrganizationGetResponse {
	pub output: Option<tg::organization::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ResolveRequest {
	pub arg: tg::resolve::Arg,
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ResolveResponse {
	pub output: Option<tg::resolve::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct SandboxGetRequest {
	pub arg: tg::sandbox::get::Arg,
	pub id: tg::sandbox::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct SandboxGetResponse {
	pub output: Option<tg::sandbox::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct TagGetRequest {
	pub arg: tg::tag::get::Arg,
	pub id: tg::tag::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct TagGetResponse {
	pub output: Option<tg::tag::get::Output>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct UserGetRequest {
	pub arg: tg::user::get::Arg,
	pub id: tg::user::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct UserGetResponse {
	pub output: Option<tg::user::get::Output>,
}

#[derive(Clone, Debug)]
pub(crate) struct Entry {
	pub response: Response,
	pub timestamp: i64,
}

impl Session {
	pub(crate) async fn try_get_cached_remote_response(
		&self,
		remote: &str,
		request: &Request,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Response>> {
		let ttl = match ttl {
			tg::remote::cache::Ttl::Default => Some(self.server.config.remote_cache.time_to_live),
			tg::remote::cache::Ttl::Duration(duration) => Some(duration),
			tg::remote::cache::Ttl::Infinite => None,
		};
		if ttl == Some(Duration::ZERO) {
			return Ok(None);
		}
		let Some(entry) = self.try_get_remote_cache(remote, request).await? else {
			return Ok(None);
		};
		let now = self.server.clock.unix_timestamp()?;
		let age = u64::try_from((now - entry.timestamp).max(0))
			.map(Duration::from_secs)
			.map_err(|error| tg::error!(!error, "invalid remote cache age"))?;
		if ttl.is_some_and(|ttl| age >= ttl) {
			return Ok(None);
		}
		let response = entry.response;
		if !request.matches_response(&response) {
			return Ok(None);
		}

		Ok(Some(response))
	}

	pub(crate) async fn put_cached_remote_response(
		&self,
		remote: &str,
		request: &Request,
		response: &Response,
	) -> tg::Result<()> {
		let timestamp = self.server.clock.unix_timestamp()?;
		self.put_remote_cache(remote, request, response, timestamp)
			.await
	}

	pub(crate) async fn try_get_remote_cache(
		&self,
		remote: &str,
		request: &Request,
	) -> tg::Result<Option<Entry>> {
		let request = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select response, timestamp
				from remote_cache
				where principal = {p}1 and remote = {p}2 and request = {p}3;
			"
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			response: String,
			timestamp: i64,
		}
		let row = connection
			.query_optional_into::<Row>(
				statement.into(),
				db::params![self.context.principal.to_string(), remote, request],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = row else {
			return Ok(None);
		};
		let Ok(response) = serde_json::from_str(&row.response) else {
			return Ok(None);
		};
		let entry = Entry {
			response,
			timestamp: row.timestamp,
		};

		Ok(Some(entry))
	}

	pub(crate) async fn put_remote_cache(
		&self,
		remote: &str,
		request: &Request,
		response: &Response,
		timestamp: i64,
	) -> tg::Result<()> {
		let request = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let response = serde_json::to_string(response)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache response"))?;
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into remote_cache (principal, remote, request, response, timestamp)
				values ({p}1, {p}2, {p}3, {p}4, {p}5)
				on conflict (principal, remote, request) do update
				set response = excluded.response, timestamp = excluded.timestamp;
			"
		);
		connection
			.execute(
				statement.into(),
				db::params![
					self.context.principal.to_string(),
					remote,
					request,
					response,
					timestamp
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Ok(())
	}

	pub(crate) async fn invalidate_remote_cache(&self, remote: &str) {
		if let Err(error) = self.delete_remote_cache(remote).await {
			tracing::warn!(error = %error.trace(), %remote, "failed to invalidate the remote cache");
		}
	}

	pub(crate) async fn delete_remote_cache(&self, remote: &str) -> tg::Result<()> {
		let connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = format!("delete from remote_cache where remote = {p}1;");
		connection
			.execute(statement.into(), db::params![remote])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Ok(())
	}
}

impl Request {
	fn matches_response(&self, response: &Response) -> bool {
		matches!(
			(self, response),
			(Self::Get(_), Response::Get(_))
				| (Self::GroupGet(_), Response::GroupGet(_))
				| (Self::List(_), Response::List(_))
				| (Self::Match(_), Response::Match(_))
				| (Self::OrganizationGet(_), Response::OrganizationGet(_))
				| (Self::Resolve(_), Response::Resolve(_))
				| (Self::SandboxGet(_), Response::SandboxGet(_))
				| (Self::TagGet(_), Response::TagGet(_))
				| (Self::UserGet(_), Response::UserGet(_))
		)
	}
}

pub(crate) fn token_valid(token: Option<&tg::grant::Token>, clock: &crate::clock::Clock) -> bool {
	token.is_none_or(|token| {
		clock
			.unix_timestamp()
			.is_ok_and(|now| token.body.expires_at > now)
	})
}
