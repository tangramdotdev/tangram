use {
	crate::Session,
	indoc::formatdoc,
	serde::{Serialize, de::DeserializeOwned},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	time::OffsetDateTime,
};

#[derive(Clone, Debug)]
pub(crate) struct Entry {
	pub response: String,
	pub timestamp: i64,
}

impl Session {
	pub(crate) async fn try_get_cached_remote_response<T>(
		&self,
		remote: &str,
		request: &str,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<T>>
	where
		T: DeserializeOwned,
	{
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
		let now = OffsetDateTime::now_utc().unix_timestamp();
		let age = u64::try_from((now - entry.timestamp).max(0))
			.map(Duration::from_secs)
			.map_err(|error| tg::error!(!error, "invalid remote cache age"))?;
		if ttl.is_some_and(|ttl| age >= ttl) {
			return Ok(None);
		}
		let response = serde_json::from_str(&entry.response)
			.map_err(|error| tg::error!(!error, "failed to deserialize the remote cache"))?;

		Ok(Some(response))
	}

	pub(crate) async fn put_cached_remote_response<T>(
		&self,
		remote: &str,
		request: &str,
		response: &T,
	) -> tg::Result<()>
	where
		T: Serialize,
	{
		let response = serde_json::to_string(response)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache"))?;
		let timestamp = OffsetDateTime::now_utc().unix_timestamp();
		self.put_remote_cache(remote, request, &response, timestamp)
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

pub(crate) fn request(operation: &str, arg: &impl Serialize) -> String {
	let arg = serde_json::to_string(arg).unwrap();
	format!("{operation}:{arg}")
}

pub(crate) fn token_valid(token: Option<&tg::grant::Token>) -> bool {
	token.is_none_or(|token| {
		token.body.expires_at > time::OffsetDateTime::now_utc().unix_timestamp()
	})
}
