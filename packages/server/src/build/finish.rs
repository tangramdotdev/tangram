use std::str::FromStr;

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
			.connection()
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
					outcome: tg::build::outcome::Data::Cancelation(
						tg::build::outcome::data::Cancelation {
							reason: Some("the build's parent was canceled".to_owned()),
						},
					),
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
			.any(|outcome| outcome.try_unwrap_cancelation_ref().is_ok())
		{
			outcome =
				tg::build::outcome::Data::Cancelation(tg::build::outcome::data::Cancelation {
					reason: Some("one of the build's children was canceled".to_owned()),
				});
		}

		// Verify the checksum if one was provided.
		let target = tg::Target::with_id(output.target);
		if let (tg::build::outcome::Data::Success(outcome_data), Some(expected)) =
			(outcome.clone(), target.checksum(self).await?.clone())
		{
			let verify_checksum = match expected {
				tg::Checksum::Unsafe => Ok(()),
				tg::Checksum::Blake3(_)
				| tg::Checksum::Sha256(_)
				| tg::Checksum::Sha512(_)
				| tg::Checksum::None => {
					async {
						// Create the checksum target.
						let host = "builtin";
						let value: tg::Value = outcome_data.value.try_into()?;
						let algorithm = if expected.algorithm() == tg::checksum::Algorithm::None {
							tg::checksum::Algorithm::Sha256
						} else {
							expected.algorithm()
						};
						let args = vec![
							"checksum".into(),
							value.clone(),
							algorithm.to_string().into(),
						];
						let target = tg::Target::builder(host).args(args).build();
						let target_id = target.id(self).await?;

						// Get the checksum child build.
						let arg = tg::target::build::Arg {
							create: false,
							parent: Some(id.clone()),
							..Default::default()
						};
						let Some(output) = self.try_build_target(&target_id, arg).await? else {
							return Err(tg::error!("could not find the checksum child build"));
						};

						// Get the checksum child build output.
						let Some(checksum_output) =
							self.try_get_build_outcome_future(&output.build).await?
						else {
							return Err(tg::error!("could not find the checksum child build"));
						};
						let Some(outcome) = checksum_output.await? else {
							return Err(tg::error!("could not get the checksum build outcome"));
						};
						let outcome = outcome.data(self).await?;
						let Ok(outcome) = outcome.try_unwrap_success_ref().cloned() else {
							return Err(tg::error!("checksum build failed"));
						};

						// Extract and compare the checksum from the child build.
						let value: tg::Value = outcome.value.try_into()?;
						let tg::Value::String(checksum) = value else {
							return Err(tg::error!("could not deserialize the checksum"));
						};
						let checksum = tg::Checksum::from_str(checksum.as_str())?;
						if expected == tg::Checksum::None {
							Err(tg::error!("no checksum provided, expected {checksum}"))
						} else if checksum == expected {
							Ok(())
						} else {
							Err(tg::error!("checksums do not match"))
						}
					}
					.await
				},
			};

			// Fail the build if checksum verification failed.
			if let Err(error) = verify_checksum {
				outcome =
					tg::build::outcome::Data::Failure(tg::build::outcome::data::Failure { error });
			}
		}

		// Create a blob from the log.
		let reader = log::Reader::new(self, id).await?;
		let log = tg::Blob::with_reader(self, reader).await?;
		let log = log.id(self).await?;

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
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
			.try_unwrap_success_ref()
			.map(|success| success.value.children())
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
		let params = db::params![log, db::value::Json(outcome), status, finished_at, id];
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
