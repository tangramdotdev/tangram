use {
	crate::{
		Session,
		context::{Authentication, Process, Sandbox},
	},
	indoc::formatdoc,
	std::sync::Arc,
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
		let Some(authentication) = self.context.authentication.as_ref() else {
			return self.list_remotes_root().await;
		};
		match authentication {
			Authentication::Process(process) => self.list_remotes_process(process).await,
			Authentication::Root => self.list_remotes_root().await,
			Authentication::Runner => self.list_remotes_runner().await,
			Authentication::Sandbox(sandbox) => self.list_remotes_sandbox(sandbox).await,
			Authentication::User(user) => self.list_remotes_user(user).await,
		}
	}

	async fn list_remotes_process(
		&self,
		process: &Arc<Process>,
	) -> tg::Result<tg::remote::list::Output> {
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
				select name, url
				from remotes
				where
					({p}1 is null or name = {p}1) and (
						("user" is null and {p}2 is null) or
						"user" = {p}2
					)
				order by name;
			"#,
		);
		let params = db::params![remote, user];
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

	async fn list_remotes_root(&self) -> tg::Result<tg::remote::list::Output> {
		let connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let statement = r#"
			select name, url
			from remotes
			where "user" is null
			order by name;
		"#;
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

	async fn list_remotes_sandbox(
		&self,
		sandbox: &Sandbox,
	) -> tg::Result<tg::remote::list::Output> {
		match &sandbox.location {
			tg::Location::Local(_) => self.list_remotes_sandbox_local(sandbox).await,
			tg::Location::Remote(remote) => self.list_remotes_sandbox_remote(&remote.name).await,
		}
	}

	async fn list_remotes_sandbox_local(
		&self,
		sandbox: &Sandbox,
	) -> tg::Result<tg::remote::list::Output> {
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
		let created_by = connection
			.query_optional_into::<SandboxRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.map_or_else(|| sandbox.created_by.clone(), |row| row.created_by);
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
				select name, url
				from remotes
				where
					("user" is null and {p}1 is null) or
					"user" = {p}1
				order by name;
			"#,
		);
		let user = created_by.as_ref().map(ToString::to_string);
		let params = db::params![user];
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

	async fn list_remotes_sandbox_remote(
		&self,
		remote: &str,
	) -> tg::Result<tg::remote::list::Output> {
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

	async fn list_remotes_user(&self, user: &tg::User) -> tg::Result<tg::remote::list::Output> {
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
		let params = db::params![user.id.to_string()];
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
