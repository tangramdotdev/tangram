use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
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
			.sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into sandboxes (
					id,
					cpu,
					created_at,
					hostname,
					isolation,
					memory,
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
					{p}8,
					{p}9,
					{p}10,
					{p}11
				);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let isolation = Self::resolve_sandbox_isolation(arg.isolation)?;
		Self::validate_sandbox_resources(isolation, arg.cpu, arg.memory)?;
		let cpu = arg
			.cpu
			.map(i64::try_from)
			.transpose()
			.map_err(|source| tg::error!(!source, "invalid sandbox cpu"))?;
		let memory = arg
			.memory
			.map(i64::try_from)
			.transpose()
			.map_err(|source| tg::error!(!source, "invalid sandbox memory"))?;
		let ttl =
			i64::try_from(arg.ttl).map_err(|source| tg::error!(!source, "invalid sandbox ttl"))?;
		let params = db::params![
			id.to_string(),
			cpu,
			now,
			arg.hostname.clone(),
			isolation.to_string(),
			memory,
			(!arg.mounts.is_empty()).then(|| db::value::Json(arg.mounts.clone())),
			arg.network,
			tg::sandbox::Status::Created.to_string(),
			ttl,
			arg.user.clone(),
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);

		self.publish_sandbox_status(&id);
		self.spawn_publish_sandboxes_created_message_task();

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
