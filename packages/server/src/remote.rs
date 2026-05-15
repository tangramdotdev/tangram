use {
	crate::{Server, Session, context::Authentication},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_uri::Uri,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

#[derive(Clone)]
pub(crate) struct Remote {
	pub name: String,
	pub url: Uri,
	pub token: Option<String>,
}

impl Session {
	pub(crate) fn authorize_remote_management(&self) -> tg::Result<()> {
		self.remote_user()?;
		Ok(())
	}

	pub async fn get_remote_session(&self, remote: String) -> tg::Result<tg::Session> {
		let client = self.get_remote_client(remote).await?;
		Ok(client.session(client.context()))
	}

	pub async fn get_remote_client(&self, remote: String) -> tg::Result<tg::Client> {
		self.try_get_remote_client(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_client(&self, remote: String) -> tg::Result<Option<tg::Client>> {
		let Some(output) = self
			.try_get_remote_config(&remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let key = output.url.clone();
		if let Some(client) = self.server.remote_clients.get(&key) {
			return Ok(Some(client.clone()));
		}
		let client = self
			.server
			.create_remote_client(output.url.clone(), output.token)?;
		self.server.remote_clients.insert(key, client.clone());
		Ok(Some(client))
	}

	pub(crate) fn remote_user(&self) -> tg::Result<Option<tg::user::Id>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.remote_user_from_authentication()
	}

	pub(crate) async fn remote_user_for_lookup(&self) -> tg::Result<Option<tg::user::Id>> {
		if self.context.authentication.is_none() && self.context.token.is_none() {
			return Ok(None);
		}
		match self.remote_user_from_authentication() {
			Ok(user) => Ok(user),
			Err(error) if self.context.process.is_some() => {
				let _ = error;
				self.try_get_process_remote_user().await
			},
			Err(error) => Err(error),
		}
	}

	fn remote_user_from_authentication(&self) -> tg::Result<Option<tg::user::Id>> {
		match &self.context.authentication {
			Some(Authentication::User(user)) => Ok(Some(user.id.clone())),
			Some(authentication) if authentication.is_root() => Ok(None),
			Some(authentication) if authentication.is_runner() || authentication.is_sandbox() => {
				Err(tg::error!("forbidden"))
			},
			_ => Err(tg::error!("forbidden")),
		}
	}

	async fn try_get_process_remote_user(&self) -> tg::Result<Option<tg::user::Id>> {
		let Some(process) = &self.context.process else {
			return Err(tg::error!("forbidden"));
		};
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select created_by
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![process.id.to_string()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let row = row.ok_or_else(|| tg::error!("failed to find the process"))?;
		Ok(row.created_by)
	}
}

impl Server {
	pub(crate) fn create_remote_client(
		&self,
		url: Uri,
		token: Option<String>,
	) -> tg::Result<tg::Client> {
		tg::Client::new(tg::Arg {
			url: Some(url),
			version: Some(self.version.clone()),
			token,
			process: None,
			pool: None,
			reconnect: None,
			retry: None,
		})
	}
}
