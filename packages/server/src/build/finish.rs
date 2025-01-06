use crate::Server;
use bytes::Bytes;
use futures::{stream::FuturesUnordered, FutureExt as _, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use tangram_client::{self as tg, handle::Ext as _};
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
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::build::finish::Arg {
				remote: None,
				..arg
			};
			let output = client.finish_build(id, arg).await?;
			return Ok(output);
		}

		// Get the build.
		let Some(build) = self.try_get_build_local(id).await? else {
			return Err(tg::error!("failed to find the build"));
		};

		// If the build is finished, then return.
		let status = self
			.try_get_current_build_status_local(id)
			.await?
			.ok_or_else(|| tg::error!(%build = id, "build does not exist"))?;
		if status.is_finished() {
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
			.query_all_value_into::<tg::build::Id>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Cancel unfinished children.
		children
			.iter()
			.map(|child| async move {
				let arg = tg::build::finish::Arg {
					error: Some(tg::error!("the parent was finished")),
					output: None,
					remote: None,
					status: tg::build::Status::Canceled,
				};
				self.finish_build(child, arg).await?;
				Ok::<_, tg::Error>(())
			})
			.collect::<FuturesUnordered<_>>()
			.collect::<Vec<_>>()
			.await;

		// Get the output.
		let mut status = arg.status;
		let output = arg.output;
		let mut error = arg.error;

		// If any of the children were canceled, then this build should be canceled.
		if children
			.iter()
			.map(|child| self.get_current_build_status_local(child))
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?
			.iter()
			.any(tg::build::Status::is_canceled)
		{
			status = tg::build::status::Status::Canceled;
			error = Some(tg::error!("one of the build's children was canceled"));
		}

		// Verify the checksum if one was provided.
		let target = tg::Target::with_id(build.target);
		if let (Some(output), Some(expected)) =
			(output.clone(), target.checksum(self).await?.clone())
		{
			match expected {
				tg::Checksum::Unsafe => (),
				tg::Checksum::Blake3(_)
				| tg::Checksum::Sha256(_)
				| tg::Checksum::Sha512(_)
				| tg::Checksum::None => {
					let value: tg::Value = output.try_into()?;
					if let Err(checksum_error) = self
						.verify_checksum(id.clone(), &value, &expected)
						.boxed()
						.await
					{
						status = tg::build::status::Status::Failed;
						error = Some(checksum_error);
					};
				},
			};
		}

		// Create a blob from the log.
		let log_path = self.logs_path().join(id.to_string());
		let tg::blob::create::Output { blob: log, .. } = if tokio::fs::try_exists(&log_path)
			.await
			.map_err(|source| {
			tg::error!(!source, "failed to determine if the path exists")
		})? {
			let output = self.create_blob_with_path(&log_path).await?;
			tokio::fs::remove_file(&log_path).await.ok();
			output
		} else {
			let reader = crate::build::log::Reader::new(self, id).await?;
			self.create_blob_with_reader(reader).await?
		};

		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

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

		// Add the output's children to the build objects.
		let objects = output
			.as_ref()
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
					error = {p}1,
					finished_at = {p}2,
					heartbeat_at = null,
					log = {p}3,
					output = {p}4,
					status = {p}5
				where id = {p}6;
			"
		);
		let finished_at = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![
			error.map(db::value::Json),
			finished_at,
			log,
			output.map(db::value::Json),
			status,
			id
		];
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

	async fn verify_checksum(
		&self,
		parent_build_id: tg::build::Id,
		value: &tg::Value,
		expected: &tg::Checksum,
	) -> tg::Result<()> {
		// Create the target.
		let host = "builtin";
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

		// Build the target.
		let arg = tg::target::build::Arg {
			create: false,
			parent: Some(parent_build_id),
			..Default::default()
		};
		let output = self.build_target(&target_id, arg).await?;
		let output = self.get_build(&output.build).await?;
		let output = tg::Build::with_id(output.id).output(self).boxed().await?;

		// Compare the checksum from the build.
		let checksum = output
			.try_unwrap_string()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?;
		let checksum = checksum.parse::<tg::Checksum>()?;
		if *expected == tg::Checksum::None {
			return Err(tg::error!("no checksum provided, actual {checksum}"));
		} else if checksum != *expected {
			return Err(tg::error!(
				"checksums do not match, expected {expected}, actual {checksum}"
			));
		}

		Ok(())
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
