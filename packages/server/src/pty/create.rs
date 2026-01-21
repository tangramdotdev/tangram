use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database, Query},
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub(crate) async fn create_pty_with_context(
		&self,
		context: &Context,
		arg: tg::pty::create::Arg,
	) -> tg::Result<tg::pty::create::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::pty::create::Arg {
				local: None,
				remotes: None,
				size: arg.size,
			};
			return client
				.create_pty(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the pty on the remote"));
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the pty.
		let id = tg::pty::Id::new();
		let pty = super::Pty::new(self, arg.size)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pty"))?;

		// Update the the database.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into ptys
				values ({p}1, {p}2, {p}3);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = db::params![
			id.to_string(),
			now,
			serde_json::to_string(&arg.size).unwrap()
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Update the server.
		self.ptys.insert(id.clone(), pty);

		// Create the output.
		let output = tg::pty::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Create the pty.
		let output = self
			.create_pty_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pty"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), Body::with_bytes(body))
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
