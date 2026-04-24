use {
	crate::{Context, Server, database::Transaction},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub(super) struct InnerArg {
	pub(super) checksum: Option<String>,
	pub(super) error: Option<String>,
	pub(super) error_code: Option<String>,
	pub(super) exit: i64,
	pub(super) now: i64,
	pub(super) output: Option<String>,
}

impl Server {
	pub(crate) async fn try_finish_process_with_context(
		&self,
		context: &Context,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
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
					.try_finish_process_local(id, arg.clone())
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_finish_process_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to finish the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_finish_process_remotes(id, arg, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to finish the process in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_finish_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<Option<()>> {
		let tg::process::finish::Arg {
			mut error,
			output,
			mut exit,
			..
		} = arg;

		// Get the process.
		let Some(tg::process::get::Output { data, .. }) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
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

		// Store the error if necessary.
		error = match error {
			Some(error) => {
				let error = self.store_process_error(error).await;
				Some(error)
			},
			None => None,
		};

		// Get a process store connection.
		let mut connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "faile to acquire a transaction"))?;

		let finished = self
			.try_finish_process_inner(&transaction, id, arg.checksum.as_ref(), error, output, exit)
			.await
			.map_err(|source| tg::error!(!source, "failed to finish the process"))?;
		if !finished {
			return Err(tg::error!("the process was already finished"));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		self.spawn_process_finish_tasks(id);

		Ok(Some(()))
	}

	pub(crate) async fn store_process_error(
		&self,
		error: tg::Either<tg::error::Data, tg::error::Id>,
	) -> tg::Either<tg::error::Data, tg::error::Id> {
		let tg::Either::Left(mut data) = error else {
			return error;
		};
		if !self.config.advanced.internal_error_locations {
			data.remove_internal_locations();
		}

		let object = match tg::error::Object::try_from_data(data.clone()) {
			Ok(object) => object,
			Err(source) => {
				let error = tg::error!(!source, "failed to create the error object");
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

	async fn try_finish_process_inner(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::process::Id,
		checksum: Option<&tg::Checksum>,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		output: Option<tg::value::Data>,
		exit: u8,
	) -> tg::Result<bool> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let error_code = error.as_ref().and_then(|error| match error {
			tg::Either::Left(data) => data.code.map(|code| code.to_string()),
			tg::Either::Right(_) => None,
		});
		let error = error.as_ref().map(|error| match error {
			tg::Either::Left(data) => serde_json::to_string(data).unwrap(),
			tg::Either::Right(id) => id.to_string(),
		});
		let output = output
			.as_ref()
			.map(serde_json::to_string)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		let arg = InnerArg {
			checksum: checksum.map(ToString::to_string),
			error,
			error_code,
			exit: exit.into(),
			now,
			output,
		};
		match transaction {
			#[cfg(feature = "postgres")]
			Transaction::Postgres(transaction) => {
				self.try_finish_process_inner_postgres(transaction, id, arg)
					.await
			},
			#[cfg(feature = "sqlite")]
			Transaction::Sqlite(transaction) => {
				self.try_finish_process_inner_sqlite(transaction, id, arg)
					.await
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
			self.spawn_publish_process_stdio_close_message_task(id, stream);
		}

		// Spawn a task to publish the status.
		self.spawn_publish_process_status_task(id);

		// Spawn a task to publish the finalize message.
		self.spawn_publish_process_finalize_message_task();

		// Spawn a task to cancel the children.
		self.spawn_cancel_process_children_task(id);
	}

	fn spawn_cancel_process_children_task(&self, id: &tg::process::Id) {
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				let result = server.cancel_process_children(&id).await;
				if let Err(error) = result {
					tracing::error!(error = %error.trace(), "failed to cancel the children");
				}
			}
		});
	}

	async fn cancel_process_children(&self, id: &tg::process::Id) -> tg::Result<()> {
		let connection =
			self.process_store.connection().await.map_err(|source| {
				tg::error!(!source, "failed to get a process store connection")
			})?;
		let p = connection.p();

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			token: Option<String>,
		}
		let statement = formatdoc!(
			"
				select process_children.child, process_children.token
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		drop(connection);

		children
			.into_iter()
			.map(|row| {
				let id = row.child;
				let token = row.token;
				async move {
					if let Some(token) = token {
						let arg = tg::process::cancel::Arg {
							location: Some(
								tg::Location::Local(tg::location::Local::default()).into(),
							),
							token,
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
	) -> tg::Result<Option<()>> {
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

	async fn try_finish_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::finish::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(()) = client
			.try_finish_process(id, arg)
			.await
			.map_err(|source| tg::error!(!source, %region, "failed to finish the process"))?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_finish_process_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
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

	async fn try_finish_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::finish::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(()) = client.try_finish_process(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to finish the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_finish_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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

		// Finish the process.
		let Some(()) = self
			.try_finish_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to finish the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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
