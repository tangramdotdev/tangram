use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub(crate) async fn try_heartbeat_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::heartbeat::Arg,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self.heartbeat_sandbox_local(id).await;
		}

		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			return self.heartbeat_sandbox_remote(id, remote).await;
		}

		Ok(None)
	}

	async fn heartbeat_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let connection = self
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update sandboxes
				set heartbeat_at = case when status = 'started' then {p}1 else heartbeat_at end
				where id = {p}2;
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![now, id.to_string()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}
		drop(connection);
		let status = self.get_sandbox_status_local(id).await?;
		Ok(Some(tg::sandbox::heartbeat::Output { status }))
	}

	async fn heartbeat_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: String,
	) -> tg::Result<Option<tg::sandbox::heartbeat::Output>> {
		let client = self
			.get_remote_client(remote)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
		let arg = tg::sandbox::heartbeat::Arg {
			local: None,
			remotes: None,
		};
		client.try_heartbeat_sandbox(id, arg).await
	}

	pub(crate) async fn handle_heartbeat_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let Some(output) = self
			.try_heartbeat_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to heartbeat the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
