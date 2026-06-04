use {
	crate::{
		Server,
		context::{Authentication, Process, Sandbox},
	},
	indoc::formatdoc,
	std::sync::Arc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn authenticate(
		&self,
		sandbox: bool,
		token: Option<&str>,
	) -> tg::Result<Option<Authentication>> {
		if sandbox {
			let Some(token) = token else {
				return Ok(None);
			};
			return self
				.authenticate_process(token)
				.await
				.map(|process| process.map(|process| Authentication::Process(Arc::new(process))));
		}

		if let Some(token) = token {
			match self.authenticate_process(token).await {
				Ok(Some(process)) => {
					return Ok(Some(Authentication::Process(Arc::new(process))));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_runner(token).await {
				Ok(true) => {
					return Ok(Some(Authentication::Runner));
				},
				Ok(false) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_sandbox(token).await {
				Ok(Some(sandbox)) => {
					return Ok(Some(Authentication::Sandbox(sandbox)));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}

			match self.authenticate_user(token).await {
				Ok(Some(user)) => {
					return Ok(Some(Authentication::User(user)));
				},
				Ok(None) => (),
				Err(error) => {
					return Err(error);
				},
			}
		}

		if self.config().authentication.is_none() {
			return Ok(Some(Authentication::Root));
		}

		Ok(None)
	}

	pub(crate) async fn authenticate_process(&self, token: &str) -> tg::Result<Option<Process>> {
		// if let Some(process) = self.sandboxes.iter().find_map(|sandbox| {
		// 	sandbox.get_process(token).map(|process| Process {
		// 		created_by: process.created_by().cloned(),
		// 		debug: process.debug().cloned(),
		// 		id: process.id().clone(),
		// 		location: process.location().cloned(),
		// 		retry: process.retry(),
		// 		sandbox: process.sandbox().clone(),
		// 		token: process.token().to_owned(),
		// 	})
		// }) {
		// 	return Ok(Some(process));
		// }

		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Debug>>")]
			debug: Option<tg::process::Debug>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			retry: bool,
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
			token: String,
		}
		let p = connection.p();
		let statement = formatdoc!(
			r"
				select
					sandboxes.created_by,
					processes.debug,
					processes.id,
					processes.retry,
					processes.sandbox,
					process_tokens.token
				from process_tokens
				join processes on processes.id = process_tokens.process
				join sandboxes on sandboxes.id = processes.sandbox
				where process_tokens.token = {p}1;
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

		let process = Process {
			created_by: row.created_by,
			debug: row.debug,
			id: row.id,
			location: self
				.config
				.runner
				.as_ref()
				.and_then(|runner| runner.remote.clone())
				.map(|name| tg::Location::Remote(tg::location::Remote { name, region: None })),
			retry: row.retry,
			sandbox: row.sandbox,
			token: row.token,
		};

		Ok(Some(process))
	}

	pub(crate) async fn authenticate_runner(&self, token: &str) -> tg::Result<bool> {
		let connection = self
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			id: String,
		}

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id
				from runner_tokens
				where id = {p}1;
			"
		);
		let params = db::params![token];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let output = row.is_some_and(|row| row.id == token);

		Ok(output)
	}

	pub(crate) async fn authenticate_sandbox(&self, token: &str) -> tg::Result<Option<Sandbox>> {
		// if let Some(sandbox) = self.sandboxes.iter().find_map(|sandbox| {
		// 	let sandbox = sandbox.value();
		// 	(sandbox.token() == Some(token)).then(|| Sandbox {
		// 		created_by: sandbox.created_by().cloned(),
		// 		id: sandbox.id().clone(),
		// 		location: sandbox.location().clone(),
		// 		token: Some(token.to_owned()),
		// 	})
		// }) {
		// 	return Ok(Some(sandbox));
		// }

		let connection = self
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::sandbox::Id,
		}

		let statement = formatdoc!(
			"
				select sandboxes.created_by, sandboxes.id
				from sandbox_tokens
				join sandboxes on sandboxes.id = sandbox_tokens.sandbox
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
		let sandbox = Sandbox {
			created_by: row.created_by,
			id: row.id,
			location: tg::Location::Local(tg::location::Local::default()),
			token: Some(token.to_owned()),
		};

		Ok(Some(sandbox))
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
				where user_tokens.id = {p}1;
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
