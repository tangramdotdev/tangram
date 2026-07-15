use {
	crate::{Server, Session},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone)]
pub(crate) struct Process {
	pub debug: Option<tg::process::Debug>,
	pub location: Option<tg::Location>,
	pub retry: bool,
	pub sandbox: tg::sandbox::Id,
	pub token: Option<String>,
}

#[derive(Clone)]
pub(crate) struct Sandbox {
	pub location: tg::Location,
	pub token: Option<String>,
}

impl Session {
	pub(crate) async fn try_get_authenticated_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Process>> {
		if let Some(sandbox) = self
			.server
			.runner
			.state
			.processes
			.get(id)
			.map(|sandbox| sandbox.value().clone())
			&& let Some(sandbox) = self.server.runner.state.sandboxes.get(&sandbox)
			&& let Some(process) = sandbox.processes.get(id)
		{
			return Ok(Some(Process {
				debug: process.data.debug.clone(),
				location: sandbox.data.location.clone(),
				retry: process.data.retry,
				sandbox: process.data.sandbox.clone(),
				token: Some(process.token.clone()),
			}));
		}

		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::Json<tg::process::Debug>>")]
			debug: Option<tg::process::Debug>,
			retry: bool,
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					processes.debug,
					processes.retry,
					processes.sandbox
				from processes
				where processes.id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let location = self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.clone())
			.map(|name| tg::Location::Remote(tg::location::Remote { name, region: None }));
		Ok(Some(Process {
			debug: row.debug,
			location,
			retry: row.retry,
			sandbox: row.sandbox,
			token: None,
		}))
	}

	pub(crate) async fn try_get_authenticated_sandbox(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<Sandbox>> {
		if let Some(sandbox) = self.server.runner.state.sandboxes.get(id) {
			let location = sandbox
				.data
				.location
				.clone()
				.ok_or_else(|| tg::error!(%id, "missing the sandbox location"))?;
			return Ok(Some(Sandbox {
				location,
				token: sandbox.token.clone(),
			}));
		}

		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select 1
				from sandboxes
				where sandboxes.id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		if connection
			.query_optional(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.is_none()
		{
			return Ok(None);
		}
		let location = self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.clone())
			.map_or_else(
				|| tg::Location::Local(tg::location::Local::default()),
				|name| tg::Location::Remote(tg::location::Remote { name, region: None }),
			);
		Ok(Some(Sandbox {
			location,
			token: None,
		}))
	}

	pub(crate) async fn try_get_authenticated_principal_remote_token(
		&self,
		remote: &str,
	) -> tg::Result<Option<String>> {
		match &self.context.principal {
			tg::Principal::Process(id) => {
				let Some(process) = self.try_get_authenticated_process(id).await? else {
					return Ok(None);
				};
				let Some(location) = process.location.as_ref() else {
					return Ok(None);
				};
				let tg::Location::Remote(location) = location else {
					return Ok(None);
				};
				if location.name != remote {
					return Ok(None);
				}
				Ok(process.token)
			},
			tg::Principal::Runner(id) => {
				let Some(runner) = self.server.config.runner.as_ref() else {
					return Ok(None);
				};
				if runner.remote.as_deref() != Some(remote) {
					return Ok(None);
				}
				if runner.id.as_ref().is_some_and(|runner| runner != id) {
					return Ok(None);
				}
				Ok(runner.token.clone())
			},
			tg::Principal::Sandbox(sandbox) => {
				let Some(sandbox) = self.try_get_authenticated_sandbox(sandbox).await? else {
					return Ok(None);
				};
				let tg::Location::Remote(location) = &sandbox.location else {
					return Ok(None);
				};
				Ok((location.name == remote).then_some(sandbox.token).flatten())
			},
			tg::Principal::Anonymous
			| tg::Principal::Group(_)
			| tg::Principal::Organization(_)
			| tg::Principal::Root
			| tg::Principal::User(_) => Ok(None),
		}
	}
}

impl Server {
	pub(crate) async fn authenticate(
		&self,
		sandbox: bool,
		token: Option<&str>,
	) -> tg::Result<tg::Principal> {
		if sandbox {
			let Some(token) = token else {
				return Ok(tg::Principal::Anonymous);
			};
			return Ok(self
				.authenticate_process(token)
				.await?
				.map_or(tg::Principal::Anonymous, tg::Principal::Process));
		}

		if let Some(token) = token {
			match self.authenticate_process(token).await {
				Ok(Some(process)) => {
					return Ok(tg::Principal::Process(process));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_runner(token).await {
				Ok(Some(runner)) => {
					return Ok(tg::Principal::Runner(runner));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_sandbox(token).await {
				Ok(Some(sandbox)) => {
					return Ok(tg::Principal::Sandbox(sandbox));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_user(token).await {
				Ok(Some(user)) => {
					return Ok(tg::Principal::User(user.id));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}
		}

		if self.config().authentication.is_none() {
			return Ok(tg::Principal::Root);
		}

		Ok(tg::Principal::Anonymous)
	}

	pub(crate) async fn authenticate_process(
		&self,
		token: &str,
	) -> tg::Result<Option<tg::process::Id>> {
		if let Some(process) = self.runner.state.sandboxes.iter().find_map(|sandbox| {
			sandbox
				.processes
				.iter()
				.find_map(|(id, process)| (process.token == token).then(|| id.clone()))
		}) {
			return Ok(Some(process));
		}

		Ok(None)
	}

	pub(crate) async fn authenticate_runner(
		&self,
		token: &str,
	) -> tg::Result<Option<tg::runner::Id>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			runner: tg::runner::Id,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
				select runner
				from runner_tokens
				where token = {p}1;
			"
		);
		let params = db::params![token];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		Ok(Some(row.runner))
	}

	pub(crate) async fn authenticate_sandbox(
		&self,
		token: &str,
	) -> tg::Result<Option<tg::sandbox::Id>> {
		if let Some(sandbox) = self.runner.state.sandboxes.iter().find_map(|sandbox| {
			let sandbox = sandbox.value();
			(sandbox.token.as_deref() == Some(token)).then(|| sandbox.data.id.clone())
		}) {
			return Ok(Some(sandbox));
		}

		Ok(None)
	}

	pub(crate) async fn authenticate_user(
		&self,
		token: &str,
	) -> tg::Result<Option<tg::user::User>> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct UserRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::user::Id,
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				select users.id, nodes.name, nodes.specifier
				from users
				join nodes on nodes.id = users.id
				join user_tokens on user_tokens."user" = users.id
				where user_tokens.token = {p}1;
			"#
		);
		let params = db::params![token];
		let user = connection
			.query_optional_into::<UserRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(user) = user else {
			return Ok(None);
		};

		#[derive(db::row::Deserialize)]
		struct EmailRow {
			email: String,
		}
		let statement = formatdoc!(
			r#"
				select email
				from user_emails
				where user_emails."user" = {p}1
				order by email;
			"#
		);
		let params = db::params![user.id.to_string()];
		let rows = connection
			.query_all_into::<EmailRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let emails = rows.into_iter().map(|row| row.email).collect();
		let user = tg::User {
			emails,
			id: user.id,
			location: None,
			name: user.name,
			specifier: user.specifier,
		};

		Ok(Some(user))
	}
}
