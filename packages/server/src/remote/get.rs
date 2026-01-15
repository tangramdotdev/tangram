use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _},
	tangram_uri::Uri,
};

impl Server {
	pub(crate) async fn try_get_remote_with_context(
		&self,
		context: &Context,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			url: Uri,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select name, url
				from remotes
				where name = {p}1;
			",
		);
		let params = db::params![&name];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			url: row.url,
		});
		Ok(output)
	}

	pub(crate) async fn handle_get_remote_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		name: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the remote.
		let Some(output) = self
			.try_get_remote_with_context(context, name)
			.await
			.map_err(|source| tg::error!(!source, %name, "failed to get the remote"))?
		else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(Body::empty())
				.unwrap());
		};

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
