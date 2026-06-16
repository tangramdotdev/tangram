use {
	crate::Session,
	indoc::{formatdoc, indoc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_uri::Uri,
};

#[derive(db::row::Deserialize)]
struct Row {
	name: String,
	#[tangram_database(as = "db::value::FromStr")]
	url: Uri,
}

impl Session {
	pub(crate) async fn list_remotes(
		&self,
		_arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		let Some(principal) = self.context.principal.as_ref() else {
			return self.list_remotes_root().await;
		};
		match principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				match self.resolve_remote_principal(principal).await? {
					tg::Principal::Root => self.list_remotes_root().await,
					tg::Principal::Runner => self.list_remotes_runner().await,
					tg::Principal::User(user) => self.list_remotes_user(&user).await,
					_ => unreachable!(),
				}
			},
			tg::Principal::Root => self.list_remotes_root().await,
			tg::Principal::Runner => self.list_remotes_runner().await,
			tg::Principal::User(user) => self.list_remotes_user(user).await,
			tg::Principal::Group(_) | tg::Principal::Organization(_) => {
				Err(tg::error!("unauthorized"))
			},
		}
	}

	async fn list_remotes_root(&self) -> tg::Result<tg::remote::list::Output> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let statement = indoc!(
			r#"
				select name, url
				from remotes
				where "user" is null
				order by name;
			"#,
		);
		let rows = connection
			.query_all_into::<Row>(statement.into(), db::params![])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				token: None,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	async fn list_remotes_runner(&self) -> tg::Result<tg::remote::list::Output> {
		let Some(remote) = self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.as_deref())
		else {
			return Ok(tg::remote::list::Output { data: Vec::new() });
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
				select name, url
				from remotes
				where name = {p}1 and "user" is null
				order by name;
			"#,
		);
		let params = db::params![remote];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				token: None,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	async fn list_remotes_user(&self, user: &tg::user::Id) -> tg::Result<tg::remote::list::Output> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select name, url
				from remotes
				where "user" = {p}1
				order by name;
			"#,
		);
		let params = db::params![user.to_string()];
		let rows = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| tg::remote::get::Output {
				name: row.name,
				token: None,
				url: row.url,
			})
			.collect();
		let output = tg::remote::list::Output { data };
		Ok(output)
	}

	pub(crate) async fn list_remotes_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// List the remotes.
		let output = self
			.list_remotes(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the remotes"))?;

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
