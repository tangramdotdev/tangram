use super::log;
use crate::Server;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn finish_build(
		&self,
		id: &tg::build::Id,
		arg: tg::build::finish::Arg,
	) -> tg::Result<bool> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::build::finish::Arg {
				remote: None,
				..arg
			};
			let output = remote.finish_build(id, arg).await?;
			return Ok(output);
		}

		// Get the build.
		let Some(output) = self.try_get_build_local(id).await? else {
			return Err(tg::error!("failed to find the build"));
		};

		// If the build is finished, then return.
		let status = self
			.try_get_current_build_status_local(id)
			.await?
			.ok_or_else(|| tg::error!(%build = id, "build does not exist"))?;

		if matches!(status, tg::build::Status::Finished) {
			return Ok(false);
		}

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the children.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child
				from build_children
				where build = {p}1
				order by position;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Cancel unfinished children.
		children
			.iter()
			.map(|child| async move {
				let arg = tg::build::finish::Arg {
					outcome: tg::build::outcome::Data::Canceled,
					remote: None,
				};
				self.finish_build(child, arg).await?;
				Ok::<_, tg::Error>(())
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await
			.ok();

		// Get the outcome.
		let mut outcome = arg.outcome;

		// If any of the children were canceled, then this build should be canceled.
		let outcomes = children
			.iter()
			.map(|child_id| async move {
				// Check if the child is finished before awaiting its outcome.
				let Some(tg::build::Status::Finished) =
					self.try_get_current_build_status_local(child_id).await?
				else {
					return Ok(None);
				};

				// Get the outcome.
				let outcome = self
					.try_get_build_outcome_future(child_id)
					.await?
					.ok_or_else(|| tg::error!(%child_id, "failed to get the build"))?
					.await?
					.ok_or_else(|| tg::error!(%child_id, "expected an outcome"))?;
				Ok::<_, tg::Error>(Some(outcome))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		if outcomes
			.iter()
			.filter_map(Option::as_ref)
			.any(|outcome| outcome.try_unwrap_canceled_ref().is_ok())
		{
			outcome = tg::build::outcome::Data::Canceled;
		}

		// Verify the checksum if one was provided.
		let target = tg::Target::with_id(output.target);
		if let Some(expected) = target.checksum(self).await?.clone() {
			if let Ok(tg::value::Data::Object(object)) = outcome.try_unwrap_succeeded_ref().cloned()
			{
				if tg::artifact::Id::try_from(object.clone()).is_ok() {
					let algorithm = expected.algorithm();
					let actual = match algorithm {
						tg::checksum::Algorithm::Unsafe => tg::Checksum::Unsafe,
						_ => {
							return Err(tg::error!("unimplemented"));
						},
					};
					if expected != tg::Checksum::Unsafe && expected != actual {
						outcome = tg::build::outcome::Data::Failed(tg::error!(
							%expected,
							%actual,
							"the checksum did not match"
						));
					}
				} else if let Ok(blob) = tg::blob::Id::try_from(object.clone()) {
					let blob = tg::Blob::with_id(blob);
					let algorithm = expected.algorithm();
					let mut writer = tg::checksum::Writer::new(algorithm);
					let mut reader = blob.reader(self).await?;
					tokio::io::copy(&mut reader, &mut writer)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to copy from the reader to the writer")
						})?;
					let actual = writer.finalize();
					if expected != tg::Checksum::Unsafe && expected != actual {
						outcome = tg::build::outcome::Data::Failed(tg::error!(
							%expected,
							%actual,
							"the checksum did not match"
						));
					}
				} else {
					outcome = tg::build::outcome::Data::Failed(tg::error!("a target with a checksum must have an output that is either an artifact or a blob"));
				}
			}
		}

		// Create a blob from the log.
		let reader = log::Reader::new(self, id).await?;
		let log = tg::Blob::with_reader(self, reader).await?;
		let log = log.id(self).await?;

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Remove the log file if it exists.
		let path = self.logs_path().join(id.to_string());
		tokio::fs::remove_file(path).await.ok();

		// Remove the log from the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from build_logs
				where build = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the log object to the build objects.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into build_objects (build, object)
				values ({p}1, {p}2)
				on conflict (build, object) do nothing;
			"
		);
		let params = db::params![id, log];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the outcome's children to the build objects.
		let objects = outcome
			.try_unwrap_succeeded_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten();
		for object in objects {
			let p = connection.p();
			let statement = formatdoc!(
				"
					insert into build_objects (build, object)
					values ({p}1, {p}2)
					on conflict (build, object) do nothing;
				"
			);
			let params = db::params![id, object];
			connection
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Update the build.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update builds
				set
					heartbeat_at = null,
					log = {p}1,
					outcome = {p}2,
					status = {p}3,
					finished_at = {p}4
				where id = {p}5;
			"
		);
		let status = tg::build::Status::Finished;
		let finished_at = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![log, outcome, status, finished_at, id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Enqueue the build for indexing.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.enqueue_builds_for_indexing(&[id])
					.await
					.inspect_err(|error| tracing::error!(?error))
					.ok();
			}
		});

		// Publish the status message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("builds.{id}.status"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(true)
	}
}

impl Server {
	pub(crate) async fn handle_finish_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.finish_build(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
