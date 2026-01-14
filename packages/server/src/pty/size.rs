use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _},
};

impl Server {
	pub async fn try_get_pty_size_with_context(
		&self,
		_context: &Context,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(size) = self
				.try_get_pty_size_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the pty size"))?
		{
			return Ok(Some(size));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(size) = self
			.try_get_pty_size_remote(id, arg.clone(), &remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to get the pty size from the remote"),
			)? {
			return Ok(Some(size));
		}

		Ok(None)
	}

	async fn try_get_pty_size_local(&self, id: &tg::pty::Id) -> tg::Result<Option<tg::pty::Size>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::Json<tg::pty::Size>")]
			size: tg::pty::Size,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select size
				from ptys
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
		else {
			return Ok(None);
		};
		Ok(Some(row.size))
	}

	async fn try_get_pty_size_remote(
		&self,
		id: &tg::pty::Id,
		arg: tg::pty::read::Arg,
		remotes: &[String],
	) -> tg::Result<Option<tg::pty::Size>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::pty::read::Arg {
			local: None,
			remotes: None,
			..arg
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.get_pty_size(id, arg)
					.await
					.map_err(|source| tg::error!(!source, %remote, "failed to get the pty size"))
			}
			.boxed()
		});
		let Ok((size, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(size)
	}

	pub(crate) async fn handle_get_pty_size_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the pty id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Get the pty size.
		let output = self
			.try_get_pty_size_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the pty size"))?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), Body::with_bytes(body))
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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
