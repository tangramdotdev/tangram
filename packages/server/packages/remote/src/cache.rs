use {
	crate::Component,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{ops::ControlFlow, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_server_core::{
		Session,
		remote::cache::{Request, Response},
	},
};

struct Entry {
	response: Response,
	timestamp: i64,
}

#[derive(db::row::Deserialize)]
struct Row {
	response: String,
	timestamp: i64,
}

struct PutArg<'a> {
	principal: &'a str,
	remote: &'a str,
	request: &'a str,
	response: &'a str,
	timestamp: i64,
}

impl Component {
	pub(crate) async fn try_get_cached_remote_response(
		&self,
		session: &Session,
		remote: &str,
		request: &Request,
		ttl: tg::remote::cache::Ttl,
	) -> tg::Result<Option<Response>> {
		let ttl = match ttl {
			tg::remote::cache::Ttl::Default => {
				Some(session.server.config.remote_cache.time_to_live)
			},
			tg::remote::cache::Ttl::Duration(duration) => Some(duration),
			tg::remote::cache::Ttl::Infinite => None,
		};
		if ttl == Some(Duration::ZERO) {
			return Ok(None);
		}
		let Some(entry) = Self::try_get_remote_cache(session, remote, request).await? else {
			return Ok(None);
		};
		let now = session.server.clock.unix_timestamp();
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
		session: &Session,
		remote: &str,
		request: &Request,
		response: &Response,
	) -> tg::Result<()> {
		let timestamp = session.server.clock.unix_timestamp();
		Self::put_remote_cache(session, remote, request, response, timestamp).await
	}

	async fn try_get_remote_cache(
		session: &Session,
		remote: &str,
		request: &Request,
	) -> tg::Result<Option<Entry>> {
		let request = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let principal = session.context.principal.to_string();
		let remote = remote.to_owned();
		let row = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				let request = request.clone();
				async move {
					Self::try_get_remote_cache_with_transaction(
						transaction,
						&principal,
						&remote,
						&request,
					)
					.await
				}
				.boxed()
			})
			.await?;
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

	async fn try_get_remote_cache_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		principal: &str,
		remote: &str,
		request: &str,
	) -> tg::Result<ControlFlow<Option<Row>, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select response, timestamp
				from remote_cache
				where principal = {p}1 and remote = {p}2 and request = {p}3;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![principal, remote, request])
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(row))
	}

	async fn put_remote_cache(
		session: &Session,
		remote: &str,
		request: &Request,
		response: &Response,
		timestamp: i64,
	) -> tg::Result<()> {
		let request = serde_json::to_string(request)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache request"))?;
		let response = serde_json::to_string(response)
			.map_err(|error| tg::error!(!error, "failed to serialize the remote cache response"))?;
		let principal = session.context.principal.to_string();
		let remote = remote.to_owned();
		session
			.server
			.database
			.run(|transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				let request = request.clone();
				let response = response.clone();
				async move {
					let arg = PutArg {
						principal: &principal,
						remote: &remote,
						request: &request,
						response: &response,
						timestamp,
					};
					Self::put_remote_cache_with_transaction(transaction, arg).await
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn put_remote_cache_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: PutArg<'_>,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let PutArg {
			principal,
			remote,
			request,
			response,
			timestamp,
		} = arg;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into remote_cache (principal, remote, request, response, timestamp)
				values ({p}1, {p}2, {p}3, {p}4, {p}5)
				on conflict (principal, remote, request) do update
				set response = excluded.response, timestamp = excluded.timestamp;
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![principal, remote, request, response, timestamp],
			)
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn invalidate_remote_cache(&self, session: &Session, remote: &str) {
		if let Err(error) = self.delete_remote_cache(session, remote).await {
			tracing::warn!(error = %error.trace(), %remote, "failed to invalidate the remote cache");
		}
	}

	pub(crate) async fn delete_remote_cache(
		&self,
		session: &Session,
		remote: &str,
	) -> tg::Result<()> {
		let remote = remote.to_owned();
		session
			.server
			.database
			.run(|transaction| {
				let remote = remote.clone();
				async move {
					Self::delete_remote_cache_for_remote_with_transaction(transaction, &remote)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(())
	}

	async fn delete_remote_cache_for_remote_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		remote: &str,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = format!("delete from remote_cache where remote = {p}1;");
		let result = transaction
			.execute(statement.into(), db::params![remote])
			.await;
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}
}
