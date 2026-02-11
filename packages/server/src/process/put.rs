use {
	crate::{Context, Server, database::Database},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn put_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::put::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the remote client"))?;
			let arg = tg::process::put::Arg {
				data: arg.data,
				local: None,
				remotes: None,
			};
			client
				.put_process(id, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process on remote"))?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let now = time::OffsetDateTime::now_utc().unix_timestamp();

		// Insert the process into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_process_postgres(id, &arg, database, now)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the process"))?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				Self::put_process_sqlite(id, &arg, database, now)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the process"))?;
			},
		}

		// Spawn a task to index the process.
		let children = arg
			.data
			.children
			.as_ref()
			.ok_or_else(|| tg::error!("expected the children to be set"))?
			.iter()
			.map(|referent| referent.item.clone())
			.collect();
		let command = std::iter::once((
			arg.data.command.clone().into(),
			tangram_index::ProcessObjectKind::Command,
		));
		let error = arg
			.data
			.error
			.as_ref()
			.into_iter()
			.flat_map(|error| match error {
				tg::Either::Left(data) => {
					let mut children = BTreeSet::new();
					data.children(&mut children);
					children
						.into_iter()
						.map(|object| {
							let kind = tangram_index::ProcessObjectKind::Error;
							(object, kind)
						})
						.collect::<Vec<_>>()
				},
				tg::Either::Right(id) => {
					let id = id.clone().into();
					let kind = tangram_index::ProcessObjectKind::Error;
					vec![(id, kind)]
				},
			});
		let log = arg.data.log.as_ref().map(|id| {
			let id = id.clone().into();
			let kind = tangram_index::ProcessObjectKind::Log;
			(id, kind)
		});
		let mut output = BTreeSet::new();
		if let Some(data) = &arg.data.output {
			data.children(&mut output);
		}
		let output = output
			.into_iter()
			.map(|output| (output, tangram_index::ProcessObjectKind::Output));
		let objects = std::iter::empty()
			.chain(command)
			.chain(error)
			.chain(log)
			.chain(output)
			.collect();
		let put_process_arg = tangram_index::PutProcessArg {
			children,
			stored: tangram_index::ProcessStored::default(),
			id: id.clone(),
			metadata: tg::process::Metadata::default(),
			objects,
			touched_at: now,
		};
		self.index_tasks
			.spawn(|_| {
				let server = self.clone();
				async move {
					if let Err(error) = server
						.index
						.put(tangram_index::PutArg {
							processes: vec![put_process_arg],
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

	pub(crate) async fn handle_put_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Put the process.
		self.put_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;

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

		let response = http::Response::builder().body(Body::empty()).unwrap();
		Ok(response)
	}
}
