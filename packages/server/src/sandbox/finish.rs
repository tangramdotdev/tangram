use {
	crate::{Context, Server, database::Transaction},
	futures::{StreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub(super) struct InnerArg {
	pub(super) now: i64,
}

pub(super) struct InnerOutput {
	pub(super) finished: bool,
	pub(super) unfinished_processes: Vec<tg::process::Id>,
}

impl Server {
	pub(crate) async fn try_finish_sandbox_with_context(
		&self,
		context: &Context,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::finish::Arg,
	) -> tg::Result<Option<()>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_finish_sandbox_local(id)
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to finish the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_finish_sandbox_regions(id, &arg, &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to finish the sandbox in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_finish_sandbox_remotes(id, &arg, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to finish the sandbox in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_finish_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<()>> {
		// Verify the sandbox is local.
		if !self
			.get_sandbox_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the sandbox"))?
		{
			return Ok(None);
		}

		// Get a process store connection.
		let mut connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a transaction"))?;

		let InnerOutput {
			finished,
			unfinished_processes,
		} = self
			.try_finish_sandbox_inner(&transaction, id)
			.await
			.map_err(|source| tg::error!(!source, "failed to finish the sandbox"))?;
		if !finished {
			return Err(tg::error!("the sandbox was already finished"));
		}

		// Finish the unfinished processes.
		let mut finished_processes = Vec::new();
		if !unfinished_processes.is_empty() {
			let error = tg::Either::Left(tg::error::Data {
				code: Some(tg::error::Code::Cancellation),
				message: Some("the process was canceled".into()),
				..Default::default()
			});
			let results = unfinished_processes
				.into_iter()
				.map(|process| {
					let error = error.clone();
					let transaction = &transaction;
					async move {
						let finished = self
							.try_finish_process_inner(
								transaction,
								&process,
								None,
								Some(error),
								None,
								1,
							)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to finish the process")
							})?;
						Ok::<_, tg::Error>(finished.then_some(process))
					}
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<Vec<_>>()
				.await;
			for process in results {
				let Some(process) = process? else {
					continue;
				};
				finished_processes.push(process);
			}
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		// Spawn a task to publish the status message.
		self.spawn_publish_sandbox_status_task(id);

		// Spawn a task to publish the finalize message.
		self.spawn_publish_sandbox_finalize_message_task();

		for process in &finished_processes {
			self.spawn_process_finish_tasks(process);
		}

		Ok(Some(()))
	}

	async fn try_finish_sandbox_inner(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::sandbox::Id,
	) -> tg::Result<InnerOutput> {
		let arg = InnerArg {
			now: time::OffsetDateTime::now_utc().unix_timestamp(),
		};
		match transaction {
			#[cfg(feature = "postgres")]
			Transaction::Postgres(transaction) => {
				self.try_finish_sandbox_inner_postgres(transaction, id, arg)
					.await
			},
			#[cfg(feature = "sqlite")]
			Transaction::Sqlite(transaction) => {
				self.try_finish_sandbox_inner_sqlite(transaction, id, arg)
					.await
			},
		}
	}

	async fn try_finish_sandbox_regions(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::finish::Arg,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_finish_sandbox_region(id, arg, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_finish_sandbox_region(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::finish::Arg,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::finish::Arg {
			location: Some(location.into()),
			..arg.clone()
		};
		let Some(()) = client.try_finish_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to finish the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_finish_sandbox_remotes(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::finish::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_finish_sandbox_remote(id, arg, remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_finish_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::finish::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::finish::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg.clone()
		};
		let Some(()) = client.try_finish_sandbox(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to finish the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_finish_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		let Some(()) = self
			.try_finish_sandbox_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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
