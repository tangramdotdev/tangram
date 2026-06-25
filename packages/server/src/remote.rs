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
	async fn authorize_remote_principal(
		&self,
		principal: Option<&tg::grant::Principal>,
	) -> tg::Result<()> {
		let Some(principal) = principal else {
			if matches!(self.context.principal, tg::Principal::Root) {
				return Ok(());
			}
			return Err(tg::error!("unauthorized"));
		};
		let id: tg::Id = match principal {
			tg::grant::Principal::Group(id) => id.clone().into(),
			tg::grant::Principal::Organization(id) => id.clone().into(),
			tg::grant::Principal::User(id) => id.clone().into(),
			_ => return Err(tg::error!("invalid remote principal")),
		};
		let permission = Self::write_permission_for_resource(&id)?;
		let Some(permissions) = self.authorize(id, permission).await? else {
			return Err(tg::error!("unauthorized"));
		};
		if !permissions.contains(permission) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(())
	}

	async fn resolve_remote_arg_principal(
		&self,
		principal: Option<tg::principal::Selector>,
	) -> tg::Result<Option<tg::grant::Principal>> {
		if let Some(principal) = principal {
			let Some(principal) = self.resolve_remote_principal_selector(&principal).await? else {
				return Err(tg::error!("failed to resolve the remote principal"));
			};
			self.authorize_remote_principal(principal.as_ref()).await?;
			return Ok(principal);
		}
		let principal = match &self.context.principal {
			tg::Principal::Process(_) | tg::Principal::Sandbox(_) => {
				self.resolve_remote_context_principal(&self.context.principal)
					.await?
			},
			principal => principal.clone(),
		};
		let principal = match principal {
			tg::Principal::Anonymous => return Err(tg::error!("unauthorized")),
			tg::Principal::Group(id) => Some(tg::grant::Principal::Group(id)),
			tg::Principal::Organization(id) => Some(tg::grant::Principal::Organization(id)),
			tg::Principal::Process(_) => return Err(tg::error!("unauthorized")),
			tg::Principal::Root => None,
			tg::Principal::Runner => return Err(tg::error!("unauthorized")),
			tg::Principal::Sandbox(_) => return Err(tg::error!("unauthorized")),
			tg::Principal::User(id) => Some(tg::grant::Principal::User(id)),
		};
		Ok(principal)
	}

	async fn resolve_remote_principal_selector(
		&self,
		principal: &tg::principal::Selector,
	) -> tg::Result<Option<Option<tg::grant::Principal>>> {
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		match principal {
			tg::principal::Selector::Principal(principal) => match principal {
				tg::grant::Principal::Group(id) => {
					let id = id.clone();
					let node =
						Self::try_get_node_by_id_with_transaction(&transaction, &id.clone().into())
							.await?;
					Ok(node.map(|_| Some(tg::grant::Principal::Group(id))))
				},
				tg::grant::Principal::Organization(id) => {
					let id = id.clone();
					let node =
						Self::try_get_node_by_id_with_transaction(&transaction, &id.clone().into())
							.await?;
					Ok(node.map(|_| Some(tg::grant::Principal::Organization(id))))
				},
				tg::grant::Principal::Root => Ok(Some(None)),
				tg::grant::Principal::User(id) => {
					let id = id.clone();
					let node =
						Self::try_get_node_by_id_with_transaction(&transaction, &id.clone().into())
							.await?;
					Ok(node.map(|_| Some(tg::grant::Principal::User(id))))
				},
				tg::grant::Principal::Process(_)
				| tg::grant::Principal::Public
				| tg::grant::Principal::Runner
				| tg::grant::Principal::Sandbox(_) => Err(tg::error!("invalid remote principal")),
			},
			tg::principal::Selector::Specifier(specifier) => {
				let Some(node) =
					Self::try_get_node_by_specifier_with_transaction(&transaction, specifier)
						.await?
				else {
					return Ok(None);
				};
				let principal = match node.kind {
					tg::id::Kind::Group => Some(tg::grant::Principal::Group(node.id.try_into()?)),
					tg::id::Kind::Organization => {
						Some(tg::grant::Principal::Organization(node.id.try_into()?))
					},
					tg::id::Kind::User => Some(tg::grant::Principal::User(node.id.try_into()?)),
					_ => return Err(tg::error!("invalid remote principal")),
				};
				Ok(Some(principal))
			},
		}
	}

	async fn resolve_remote_context_principal(
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
			.try_get_remote(remote, tg::remote::get::Arg::default())
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
