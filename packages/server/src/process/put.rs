use {
	crate::{Session, database::Database},
	num::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn put_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		if matches!(self.context.principal, Some(tg::Principal::Process(_))) {
			return Err(tg::error!("unauthorized"));
		}

		let location = self.server.location(arg.location.as_ref())?;

		match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.put_process_local(id, arg).await?;
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.put_process_region(id, arg, region).await?;
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.put_process_remote(id, arg, remote, region).await?;
			},
		}

		Ok(())
	}

	async fn put_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let principal = self.context.principal.clone();
		let creator = self.context.principal.clone();

		// Insert the process into the process store.
		match &self.server.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.put_process_postgres(
					id,
					&arg,
					process_store,
					now,
					principal.as_ref(),
					creator.as_ref(),
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the process"))?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.put_process_sqlite(
					id,
					&arg,
					process_store,
					now,
					principal.as_ref(),
					creator.as_ref(),
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the process"))?;
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.put_process_turso(
					id,
					&arg,
					process_store,
					now,
					principal.as_ref(),
					creator.as_ref(),
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to put the process"))?;
			},
		}

		// Spawn a task to index the process.
		let children = arg
			.data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|child| child.process.clone())
			.collect();
		let error = arg.data.error.as_ref().map(|error| match error {
			tg::Either::Left(data) => {
				let mut children = BTreeSet::new();
				data.children(&mut children);
				children.into_iter().collect::<Vec<_>>()
			},
			tg::Either::Right(id) => {
				let id = id.clone().into();
				vec![id]
			},
		});
		let mut output = BTreeSet::new();
		if let Some(data) = &arg.data.output {
			data.children(&mut output);
		}
		let output = arg
			.data
			.output
			.as_ref()
			.map(|_| output.into_iter().collect::<Vec<_>>());
		let put_process_arg = tangram_index::process::put::Arg {
			children: Some(children),
			command: arg.data.command.clone().into(),
			error: Some(error),
			id: id.clone(),
			log: Some(arg.data.log.clone().map(Into::into)),
			metadata: tg::process::Metadata::default(),
			output: Some(output),
			parent: None,
			stored: tangram_index::process::Stored::default(),
			touched_at: now,
		};
		let grant_expires_at = now
			+ self
				.server
				.config
				.process
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let put_grant =
			self.context
				.principal
				.clone()
				.map(|principal| tangram_index::grant::put::Arg {
					created_at: now,
					creator: Some(principal.clone()),
					expires_at: Some(grant_expires_at),
					permission: tg::grant::Permission::Process(
						tg::grant::permission::process::Permission::Node,
					),
					principal: principal.into(),
					resource: id.clone().into(),
				});
		self.server
			.index_tasks
			.spawn(|_| {
				let session = self.clone();
				async move {
					if let Err(error) = session
						.server
						.index
						.batch(tangram_index::batch::Arg {
							put_grants: put_grant.map(|arg| vec![arg]).unwrap_or_default(),
							put_processes: vec![put_process_arg],
							..Default::default()
						})
						.await
					{
						tracing::error!(error = %error.trace(), "failed to put process to index");
					}
				}
			})
			.detach();

		Ok(())
	}

	async fn put_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		region: String,
	) -> tg::Result<()> {
		let client = self.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::process::put::Arg {
			location: Some(location.into()),
			..arg
		};
		client
			.put_process(id, arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to put the process"))?;
		Ok(())
	}

	async fn put_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
		remote: String,
		region: Option<String>,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote).await.map_err(
			|error| tg::error!(!error, remote = %remote, %id, "failed to get the remote client"),
		)?;
		let location = region.as_deref().map_or_else(
			|| tg::Location::Local(tg::location::Local::default()),
			|region| {
				tg::Location::Local(tg::location::Local {
					region: Some(region.to_owned()),
				})
			},
		);
		let arg = tg::process::put::Arg {
			location: Some(location.into()),
			..arg
		};
		client
			.put_process(id, arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote, "failed to put the process"))?;
		Ok(())
	}

	pub(crate) async fn put_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		// Put the process.
		self.put_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the process"))?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
