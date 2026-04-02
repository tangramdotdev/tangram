use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn create_sandbox_with_context(
		&self,
		context: &Context,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let id = tg::sandbox::Id::new();
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into sandboxes (
					id,
					created_at,
					hostname,
					mounts,
					network,
					status,
					ttl,
					\"user\"
				)
				values (
					{p}1,
					{p}2,
					{p}3,
					{p}4,
					{p}5,
					{p}6,
					{p}7,
					{p}8
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			id.to_string(),
			now,
			arg.hostname.clone(),
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			tg::sandbox::Status::Created.to_string(),
			arg.ttl.map(|ttl| i64::try_from(ttl).unwrap()),
			arg.user.clone(),
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		self.publish_sandbox_status(&id);
		let subject = "sandboxes.queue".to_owned();
		let payload = crate::sandbox::queue::Message { id: id.clone() };
		self.messenger
			.stream_publish("sandboxes.queue".to_owned(), subject, payload)
			.await
			.map_err(|source| tg::error!(!source, "failed to enqueue the sandbox"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to enqueue the sandbox"))?;

		Ok(tg::sandbox::create::Output { id })
	}

	pub(crate) async fn handle_create_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		let output = self.create_sandbox_with_context(context, arg).await?;

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
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
