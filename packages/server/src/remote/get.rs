use {
	crate::{Session, remote::Remote},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_uri::Uri,
};

impl Session {
	pub(crate) async fn try_get_remote_config(&self, name: &str) -> tg::Result<Option<Remote>> {
		let owner = self.remote_owner_for_lookup().await?;

		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			url: Uri,
			token: Option<String>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select name, url, token
				from remotes
				where name = {p}1
					and (
						(\"user\" is null and {p}2 is null)
						or \"user\" = {p}2
					);
			",
		);
		let params = db::params![&name, owner];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row.map(|row| Remote {
			name: row.name,
			url: row.url,
			token: row.token,
		});
		Ok(output)
	}

	pub(crate) async fn try_get_remote(
		&self,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let output =
			self.try_get_remote_config(name)
				.await?
				.map(|remote| tg::remote::get::Output {
					name: remote.name,
					url: remote.url,
				});
		Ok(output)
	}

	pub(crate) async fn try_get_remote_request(
		&self,
		request: http::Request<BoxBody>,
		name: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the remote.
		let Some(output) = self
			.try_get_remote(name)
			.await
			.map_err(|error| tg::error!(!error, %name, "failed to get the remote"))?
		else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
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
