use {
	crate::Session,
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	time::OffsetDateTime,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Request {
	GroupGet(GroupGetRequest),
	List(ListRequest),
	Match(MatchRequest),
	OrganizationGet(OrganizationGetRequest),
	Resolve(ResolveRequest),
	TagGet(TagGetRequest),
	UserGet(UserGetRequest),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Response {
	GroupGet(Option<tg::Group>),
	List(tg::list::Output),
	Match(tg::match_::Output),
	OrganizationGet(Option<tg::Organization>),
	Resolve(Option<tg::resolve::Output>),
	TagGet(Option<tg::tag::get::Output>),
	UserGet(Option<tg::User>),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct GroupGetRequest {
	pub arg: tg::group::get::Arg,
	pub id: tg::group::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ListRequest {
	pub arg: tg::list::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct MatchRequest {
	pub arg: tg::match_::Arg,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct OrganizationGetRequest {
	pub arg: tg::organization::get::Arg,
	pub id: tg::organization::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ResolveRequest {
	pub arg: tg::resolve::Arg,
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct TagGetRequest {
	pub arg: tg::tag::get::Arg,
	pub id: tg::tag::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct UserGetRequest {
	pub arg: tg::user::get::Arg,
	pub id: tg::user::Id,
}

#[derive(Clone, Debug)]
pub(crate) struct Entry {
	pub response: String,
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
		let request_string = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let Some(entry) = self.try_get_remote_cache(remote, &request_string).await? else {
			return Ok(None);
		};
		let now = OffsetDateTime::now_utc().unix_timestamp();
		let age = u64::try_from((now - entry.timestamp).max(0))
			.map(Duration::from_secs)
			.map_err(|error| tg::error!(!error, "invalid remote cache age"))?;
		if ttl.is_some_and(|ttl| age >= ttl) {
			return Ok(None);
		}
		let Ok(response) = serde_json::from_str(&entry.response) else {
			return Ok(None);
		};
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
		let request = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let response = serde_json::to_string(response)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache response"))?;
		let timestamp = OffsetDateTime::now_utc().unix_timestamp();
		self.put_remote_cache(remote, &request, &response, timestamp)
			.await
	}

	pub(crate) async fn try_get_remote_cache(
		&self,
		remote: &str,
		request: &str,
	) -> tg::Result<Option<Entry>> {
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
		let entry = row.map(|row| Entry {
			response: row.response,
			timestamp: row.timestamp,
		});

		Ok(entry)
	}

	pub(crate) async fn put_remote_cache(
		&self,
		remote: &str,
		request: &str,
		response: &str,
		timestamp: i64,
	) -> tg::Result<()> {
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
			(Self::GroupGet(_), Response::GroupGet(_))
				| (Self::List(_), Response::List(_))
				| (Self::Match(_), Response::Match(_))
				| (Self::OrganizationGet(_), Response::OrganizationGet(_))
				| (Self::Resolve(_), Response::Resolve(_))
				| (Self::TagGet(_), Response::TagGet(_))
				| (Self::UserGet(_), Response::UserGet(_))
		)
	}
}

pub(crate) fn token_valid(token: Option<&tg::grant::Token>) -> bool {
	token.is_none_or(|token| {
		token.body.expires_at > time::OffsetDateTime::now_utc().unix_timestamp()
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn request_serializes_as_a_json_enum() {
		let id = tg::group::Id::new();
		let request = Request::GroupGet(GroupGetRequest {
			arg: tg::group::get::Arg::default(),
			id: id.clone(),
		});

		let value = serde_json::to_value(request).unwrap();

		assert_eq!(
			value,
			serde_json::json!({
				"group_get": {
					"arg": {},
					"id": id.to_string(),
				},
			}),
		);
	}

	#[test]
	fn response_serializes_as_a_json_enum() {
		let response = Response::GroupGet(None);

		let value = serde_json::to_value(response).unwrap();

		assert_eq!(value, serde_json::json!({ "group_get": null }));
	}
}
