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
		if matches!(
			principal,
			tg::Principal::Process(_) | tg::Principal::Sandbox(_)
		) && self
			.server
			.config
			.runner
			.as_ref()
			.and_then(|runner| runner.remote.as_ref())
			.is_some()
		{
			return Ok(tg::Principal::Runner);
		}

		let id = match principal {
			tg::Principal::Process(id) => id.to_string(),
			tg::Principal::Sandbox(id) => id.to_string(),
			_ => return Ok(principal.clone()),
		};
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				with recursive creators(principal) as (
					select creator from (
						select creator
						from processes
						where id = {p}1

						union

						select creator
						from sandboxes
						where id = {p}1
					) anchors

					union

					select edges.creator
					from creators
					join (
						select id, creator
						from processes

						union

						select id, creator
						from sandboxes
					) edges on edges.id = creators.principal
				)
				select principal
				from creators
				where principal in ('root', 'runner') or principal like 'usr_%'
				limit 1;
			"
		);
		let principal = connection
			.query_optional_value_into::<String>(statement.into(), db::params![id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.ok_or_else(|| tg::error!("failed to resolve the principal creator"))?;
		let principal = principal
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the principal creator"))?;
		match principal {
			tg::Principal::Root | tg::Principal::Runner | tg::Principal::User(_) => Ok(principal),
			_ => Err(tg::error!("failed to resolve the principal creator")),
		}
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
