use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_uri::Uri,
};

#[derive(db::row::Deserialize)]
struct Row {
	name: String,
	#[tangram_database(as = "db::value::FromStr")]
	url: Uri,
	token: Option<String>,
}

impl Session {
	pub(crate) async fn try_get_remote(
		&self,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let authentication = self
			.context
			.principal
			.as_ref()
			.ok_or_else(|| tg::error!("unauthenticated"))?;
		match authentication {
			tg::Principal::Process(_) | tg::Principal::Root | tg::Principal::Sandbox(_) => {
				self.try_get_remote_root(name).await
			},
			tg::Principal::Runner => self.try_get_remote_runner(name).await,
			tg::Principal::User(user) => self.try_get_remote_user(name, user).await,
			tg::Principal::Group(_) | tg::Principal::Organization(_) => {
				Err(tg::error!("unauthorized"))
			},
		}
	}

	async fn try_get_remote_root(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select name, token, url
				from remotes
				where name = {p}1 and "user" is null;
			"#,
		);
		let params = db::params![name];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			token: row.token,
			url: row.url,
		});
		Ok(output)
	}

	async fn try_get_remote_runner(
		&self,
		name: &str,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let Some(remote) = self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.as_deref())
		else {
			return Ok(None);
		};
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select name, token, url
				from remotes
				where name = {p}1 and name = {p}2 and "user" is null;
			"#,
		);
		let params = db::params![name, remote];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			token: row.token,
			url: row.url,
		});
		Ok(output)
	}

	async fn try_get_remote_user(
		&self,
		name: &str,
		user: &tg::user::Id,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select name, token, url
				from remotes
				where name = {p}1 and "user" = {p}2;
			"#,
		);
		let params = db::params![name, user.to_string()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row.map(|row| tg::remote::get::Output {
			name: row.name,
			token: row.token,
			url: row.url,
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
			let response = http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
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
