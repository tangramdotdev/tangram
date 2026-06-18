use {
	crate::{
		Session,
		database::{self, Transaction},
	},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
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

#[derive(Clone, Copy, Debug)]
pub(crate) enum Condition {
	DepthExceeded { max_depth: i64 },
	LeaseCountZero,
}

#[derive(Clone)]
pub(crate) struct InnerArg {
	pub checksum: Option<tg::Checksum>,
	pub condition: Option<Condition>,
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	pub error_code: Option<tg::error::Code>,
	pub exit: u8,
	pub now: i64,
	pub output: Option<tg::value::Data>,
}

impl Session {
	pub(crate) async fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Option<bool>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_finish_process_local(id, arg.clone(), None)
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to finish the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_finish_process_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to finish the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_finish_process_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to finish the process in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_finish_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		condition: Option<Condition>,
	) -> tg::Result<Option<bool>> {
		let tg::process::finish::Arg {
			mut error,
			mut output,
			mut exit,
			..
		} = arg;
		if let Some(output) = &mut output {
			output.remove_tokens();
		}

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?
		else {
			return Ok(None);
		};

		// Verify the checksum if one was provided.
		if let Some(expected) = &data.expected_checksum
			&& exit == 0
		{
			let actual = arg
				.checksum
				.as_ref()
				.ok_or_else(|| tg::error!(%id, "the actual checksum was not set"))?;
			if expected != actual {
				let data = tg::error::Data {
					code: Some(tg::error::Code::ChecksumMismatch),
					message: Some("checksum mismatch".into()),
					values: [
						("expected".into(), expected.to_string()),
						("actual".into(), actual.to_string()),
					]
					.into(),
					..Default::default()
				};
				error = Some(tg::Either::Left(data));
				exit = 1;
			}
		}

		let error_code = match error.as_ref() {
			Some(tg::Either::Left(data)) => data.code,
			Some(tg::Either::Right(id)) => {
				let error = tg::Error::with_id(id.clone());
				error
					.data_with_handle(self)
					.await
					.ok()
					.and_then(|data| data.code)
			},
			None => None,
		};

		// Store the error if necessary.
		error = match error {
			Some(error) => {
				let error = self.store_process_error(error).await;
				Some(error)
			},
			None => None,
		};

		let arg = InnerArg {
			checksum: arg.checksum,
			condition,
			error,
			error_code,
			exit,
			now: time::OffsetDateTime::now_utc().unix_timestamp(),
			output,
		};

		// Finish the process in the process store.
		let id = id.clone();
		let session = self.clone();
		let finished = self
			.server
			.process_store
			.run(|transaction| {
				let arg = arg.clone();
				let id = id.clone();
				let session = session.clone();
				async move {
					session
						.try_finish_process_inner(transaction, &id, arg)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to finish the process"))?;
		if !finished {
			return Ok(Some(false));
		}

		let command = data.command.clone();
		let finish_arg = arg.clone();
		let server = self.server.clone();
		let process = id.clone();
		self.server
			.index_tasks
			.spawn(|_| async move {
				let error = finish_arg.error.as_ref().map(|error| match error {
					tg::Either::Left(data) => {
						let mut children = std::collections::BTreeSet::new();
						data.children(&mut children);
						children.into_iter().collect::<Vec<_>>()
					},
					tg::Either::Right(id) => {
						let id = id.clone().into();
						vec![id]
					},
				});
				let mut output = std::collections::BTreeSet::new();
				if let Some(data) = &finish_arg.output {
					data.children(&mut output);
				}
				let output = finish_arg
					.output
					.as_ref()
					.map(|_| output.into_iter().collect::<Vec<_>>());
				let put_process_arg = tangram_index::process::put::Arg {
					children: None,
					command: command.into(),
					error: Some(error),
					id: process,
					log: None,
					metadata: tg::process::Metadata::default(),
					output: Some(output),
					parent: None,
					stored: tangram_index::process::Stored::default(),
					touched_at: time::OffsetDateTime::now_utc().unix_timestamp(),
				};
				if let Err(error) = server
					.index
					.batch(tangram_index::batch::Arg {
						put_processes: vec![put_process_arg],
						..Default::default()
					})
					.await
				{
					tracing::error!(error = %error.trace(), "failed to put process to index");
				}
			})
			.detach();

		self.spawn_process_finish_tasks(&id);

		Ok(Some(true))
	}

	pub(crate) async fn store_process_error(
		&self,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		let tg::Either::Left(mut data) = error else {
			return error;
		};
		if !self.server.config.advanced.internal_error_locations {
			data.remove_internal_locations();
		}

		let object = match tg::error::Object::try_from_data(data.clone()) {
			Ok(object) => object,
			Err(error) => {
				let error = tg::error!(!error, "failed to create the error object");
				tracing::error!(error = %error.trace(), "failed to store the process error");
				return tg::Either::Left(data);
			},
		};

		let error = tg::Error::with_object(object);
		let result = error.store_with_handle(self).await;
		match result {
			Ok(id) => tg::Either::Right(id),
			Err(error) => {
				tracing::error!(error = %error.trace(), "failed to store the process error");
				tg::Either::Left(data)
			},
		}
	}

	pub(crate) async fn try_finish_process_inner(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::process::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<bool, database::Error>> {
		match transaction {
			#[cfg(feature = "postgres")]
			Transaction::Postgres(transaction) => {
				let result = self
					.try_finish_process_inner_postgres(transaction, id, arg)
					.await;
				match result {
					Ok(ControlFlow::Break(finished)) => Ok(ControlFlow::Break(finished)),
					Ok(ControlFlow::Continue(error)) => {
						Ok(ControlFlow::Continue(database::Error::Postgres(error)))
					},
					Err(error) => Err(error),
				}
			},
			#[cfg(feature = "sqlite")]
			Transaction::Sqlite(transaction) => {
				let result = self
					.try_finish_process_inner_sqlite(transaction, id, arg)
					.await;
				match result {
					Ok(ControlFlow::Break(finished)) => Ok(ControlFlow::Break(finished)),
					Ok(ControlFlow::Continue(error)) => {
						Ok(ControlFlow::Continue(database::Error::Sqlite(error)))
					},
					Err(error) => Err(error),
				}
			},
			#[cfg(feature = "turso")]
			Transaction::Turso(transaction) => {
				let result = self
					.try_finish_process_inner_turso(transaction, id, arg)
					.await;
				match result {
					Ok(ControlFlow::Break(finished)) => Ok(ControlFlow::Break(finished)),
					Ok(ControlFlow::Continue(error)) => {
						Ok(ControlFlow::Continue(database::Error::Turso(error)))
					},
					Err(error) => Err(error),
				}
			},
		}
	}

	pub(crate) fn spawn_process_finish_tasks(&self, id: &tg::process::Id) {
		// Spawn tasks to publish the stdio close messages.
		for stream in [
			tg::process::stdio::Stream::Stdin,
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		] {
			self.server
				.spawn_publish_process_stdio_close_message_task(id, stream);
		}

		// Spawn a task to publish the status.
		self.server.spawn_publish_process_status_task(id);

		// Spawn a task to publish the finalize message.
		self.server.spawn_publish_process_finalize_message_task();

		// Spawn a task to cancel the children.
		self.spawn_cancel_process_children_task(id);
	}

	fn spawn_cancel_process_children_task(&self, id: &tg::process::Id) {
		tokio::spawn({
			let session = self.clone();
			let id = id.clone();
			async move {
				let result = session.cancel_process_children(&id).await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to cancel the children");
				}
			}
		});
	}

	async fn cancel_process_children(&self, id: &tg::process::Id) -> tg::Result<()> {
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			lease: Option<String>,
		}
		let statement = formatdoc!(
			"
				select process_children.child, process_children.lease
				from process_children
				inner join processes on processes.id = process_children.child
				where process_children.process = {p}1 and processes.status != 'finished'
				order by process_children.position;
			"
		);
		let params = db::params![id.to_string()];
		let children = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		drop(connection);

		children
			.into_iter()
			.map(|row| {
				let id = row.child;
				let lease = row.lease;
				async move {
					if let Some(lease) = lease {
						let arg = tg::process::cancel::Arg {
							location: Some(
								tg::Location::Local(tg::location::Local::default()).into(),
							),
							lease,
						};
						self.cancel_process(&id, arg).await.ok();
					}
				}
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		Ok(())
	}

	async fn try_finish_process_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_finish_process_region(id, arg.clone(), region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(error) => {
					result = Err(error);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_finish_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::finish::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(finished) = client
			.try_finish_process(id, arg)
			.await
			.map_err(|error| tg::error!(!error, %region, "failed to finish the process"))?
		else {
			return Ok(None);
		};
		Ok(Some(finished))
	}

	async fn try_finish_process_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_finish_process_remote(id, arg.clone(), remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(error) => {
					result = Err(error);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_finish_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<bool>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::finish::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(finished) = client.try_finish_process(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to finish the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(finished))
	}

	pub(crate) async fn try_finish_process_request(
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

		// Finish the process.
		let Some(finished) = self
			.try_finish_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to finish the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !finished {
			return Ok(http::Response::builder()
				.status(http::StatusCode::CONFLICT)
				.empty()
				.unwrap()
				.boxed_body());
		}

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
