use {
	crate::{Server, Session},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_uri::Uri,
};

pub mod delete;
pub mod get;
pub mod list;
pub mod put;

impl Session {
	async fn resolve_remote_principal(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<tg::Principal> {
		let (statement, id) = match principal {
			tg::Principal::Process(id) => {
				let statement = formatdoc!(
					"
						select sandboxes.owner
						from processes
						join sandboxes on sandboxes.id = processes.sandbox
						where processes.id = {{p}}1;
					"
				);
				(statement, id.to_string())
			},
			tg::Principal::Sandbox(id) => {
				let statement = formatdoc!(
					"
						select owner
						from sandboxes
						where id = {{p}}1;
					"
				);
				(statement, id.to_string())
			},
			_ => return Ok(principal.clone()),
		};
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = statement.replace("{p}", p);
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			owner: Option<tg::Principal>,
		}
		let principal = connection
			.query_optional_into::<Row>(statement.into(), db::params![id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.ok_or_else(|| tg::error!("failed to resolve the sandbox owner"))?
			.owner
			.unwrap_or(tg::Principal::Root);
		Ok(principal)
	}

	pub async fn get_remote_session(&self, remote: &str) -> tg::Result<tg::Session> {
		self.try_get_remote_session(remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub async fn try_get_remote_session(&self, remote: &str) -> tg::Result<Option<tg::Session>> {
		let Some(output) = self
			.try_get_remote(remote)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let client = self
			.server
			.get_or_create_remote_client(output.url)
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let mut context = client.context().clone();
		context.token = self
			.try_get_authenticated_principal_remote_token(remote)
			.await?
			.or(output.token);
		let session = client.session(&context);
		Ok(Some(session))
	}
}

impl Server {
	pub(crate) fn get_or_create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		if let Some(client) = self.remote_clients.get(&url) {
			return Ok(client.clone());
		}
		let client = self.create_remote_client(url.clone())?;
		self.remote_clients.insert(url, client.clone());
		Ok(client)
	}

	pub(crate) fn create_remote_client(&self, url: Uri) -> tg::Result<tg::Client> {
		tg::Client::new(tg::Arg {
			url: Some(url),
			version: Some(self.version.clone()),
			token: None,
			pool: None,
			reconnect: None,
			retry: None,
			sync: tg::sync::Config {
				max_frame_size: self.config().sync.max_frame_size,
			},
		})
	}
}
