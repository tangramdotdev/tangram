use {
	crate::{
		Session,
		context::{Authentication, Process, Sandbox},
	},
	indoc::formatdoc,
	std::sync::Arc,
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
			.authentication
			.as_ref()
			.ok_or_else(|| tg::error!("unauthenticated"))?;
		match authentication {
			Authentication::Process(process) => self.try_get_remote_process(name, process).await,
			Authentication::Root => self.try_get_remote_root(name).await,
			Authentication::Runner => self.try_get_remote_runner(name).await,
			Authentication::Sandbox(sandbox) => self.try_get_remote_sandbox(name, sandbox).await,
			Authentication::User(user) => self.try_get_remote_user(name, user).await,
		}
	}

	async fn try_get_remote_process(
		&self,
		name: &str,
		process: &Arc<Process>,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let remote = process.location.as_ref().and_then(|location| {
			let tg::Location::Remote(remote) = location else {
				return None;
			};
			Some(remote.name.clone())
		});
		let user = process.created_by.as_ref().map(ToString::to_string);
		let statement = formatdoc!(
			r#"
				select name, token, url
				from remotes
				where name = {p}1 and ({p}2 is null or name = {p}2) and (
					("user" is null and {p}3 is null) or
					"user" = {p}3
				);
			"#,
		);
		let params = db::params![name, remote, user];
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
		let remote = self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.as_deref())
			.ok_or_else(|| tg::error!("missing the runner remote"))?;
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

	async fn try_get_remote_sandbox(
		&self,
		name: &str,
		sandbox: &Sandbox,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		match &sandbox.location {
			tg::Location::Local(_) => self.try_get_remote_sandbox_local(name, sandbox).await,
			tg::Location::Remote(remote) => {
				self.try_get_remote_sandbox_remote(name, &remote.name).await
			},
		}
	}

	async fn try_get_remote_sandbox_local(
		&self,
		name: &str,
		sandbox: &Sandbox,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		#[derive(db::row::Deserialize)]
		struct SandboxRow {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select created_by
				from sandboxes
				where id = {p}1;
			"
		);
		let params = db::params![sandbox.id.to_string()];
		let Some(row) = connection
			.query_optional_into::<SandboxRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Err(tg::error!(sandbox = %sandbox.id, "failed to find the sandbox"));
		};
		drop(connection);

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
				where name = {p}1 and (
					("user" is null and {p}2 is null) or
					"user" = {p}2
				);
			"#,
		);
		let user = row.created_by.map(|user| user.to_string());
		let params = db::params![name, user];
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

	async fn try_get_remote_sandbox_remote(
		&self,
		name: &str,
		remote: &str,
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
		user: &tg::User,
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
		let params = db::params![name, user.id.to_string()];
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
