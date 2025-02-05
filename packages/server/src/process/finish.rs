use std::pin::pin;

use crate::Server;
use bytes::Bytes;
use futures::{future, stream::FuturesUnordered, FutureExt as _, TryStreamExt as _};
use indoc::formatdoc;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn try_finish_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::finish::Arg,
	) -> tg::Result<tg::process::finish::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::finish::Arg {
				remote: None,
				..arg.clone()
			};
			let output = client.try_finish_process(id, arg).await?;
			return Ok(output);
		}

		// If the process is running locally then abort it.
		self.processes.abort(id);

		// Finish the process.
		self.try_finish_process_local(id, arg.status, arg.output, arg.error, None)
			.await
	}

	pub async fn try_finish_process_local(
		&self,
		id: &tg::process::Id,
		mut status: tg::process::Status,
		output: Option<tg::value::Data>,
		mut error: Option<tg::Error>,
		exit: Option<tg::process::Exit>,
	) -> tg::Result<tg::process::finish::Output> {
		// Attempt to set the process's status to finishing.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set status = 'finishing'
				where id = {p}1 and status = 'started';
			"
		);
		let params = db::params![id];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		if n == 0 {
			return Ok(tg::process::finish::Output { finished: false });
		}

		// Get the process.
		let Some(process) = self.try_get_process_local(id).await? else {
			return Err(tg::error!("failed to find the process"));
		};

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
				from process_children
				where process = {p}1
				order by position;
			"
		);
		let params = db::params![id];
		let children = connection
			.query_all_value_into::<tg::process::Id>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Cancel unfinished children.
		children
			.clone()
			.into_iter()
			.map(|child| async move {
				let arg = tg::process::finish::Arg {
					error: Some(tg::error!("the parent was finished")),
					exit: None,
					output: None,
					remote: None,
					status: tg::process::Status::Canceled,
				};
				let output = self.try_finish_process(&child, arg).await?;
				Ok::<_, tg::Error>((child, output.finished))
			})
			.collect::<FuturesUnordered<_>>()
			.try_filter_map(|(child, finished)| future::ready(Ok(finished.then_some(child))))
			.try_collect::<Vec<_>>()
			.await?;

		// If any of the children were canceled, then this process should be canceled.
		let children_statuses = children
			.iter()
			.map(|child| async {
				let stream = self.get_process_status(child).await?;
				let status = pin!(stream).try_next().await?.unwrap();
				Ok::<_, tg::Error>(status)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		if children_statuses
			.iter()
			.any(tg::process::Status::is_canceled)
		{
			if error.is_none() {
				error = Some(tg::error!("one of the process's children was canceled"));
			}
			status = tg::process::Status::Canceled;
		}

		// Verify the checksum if one was provided.
		if let (Some(output), Some(expected)) = (output.clone(), process.checksum.as_ref()) {
			let value: tg::Value = output.try_into()?;
			if let Err(checksum_error) = self
				.verify_checksum(id.clone(), &value, expected)
				.boxed()
				.await
			{
				status = tg::process::status::Status::Failed;
				error = Some(checksum_error);
			}
		}

		// Create a blob from the log.
		let log_path = self.logs_path().join(id.to_string());
		let exists = tokio::fs::try_exists(&log_path)
			.await
			.map_err(|source| tg::error!(!source, "failed to determine if the path exists"))?;
		let tg::blob::create::Output { blob: log, .. } = if exists {
			let output = self
				.create_blob_with_path(&log_path)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the blob for the log"))?;
			tokio::fs::remove_file(&log_path)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to remove the log file"))
				.ok();
			output
		} else {
			let reader = crate::process::log::Reader::new(self, id)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to create the blob reader for the log")
				})?;
			self.create_blob_with_reader(reader)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the blob for the log"))?
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
				delete from process_logs
				where process = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the log to the process objects.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into process_objects (process, object)
				values ({p}1, {p}2)
				on conflict (process, object) do nothing;
			"
		);
		let params = db::params![id, log];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Add the output's children to the process objects.
		let objects = output
			.as_ref()
			.map(tg::value::Data::children)
			.into_iter()
			.flatten();
		for object in objects {
			let p = connection.p();
			let statement = formatdoc!(
				"
					insert into process_objects (process, object)
					values ({p}1, {p}2)
					on conflict (process, object) do nothing;
				"
			);
			let params = db::params![id, object];
			connection
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Update the process.
		let p = connection.p();
		let statement = formatdoc!(
			"
				update processes
				set
					error = {p}1,
					finished_at = {p}2,
					heartbeat_at = null,
					log = {p}3,
					output = {p}4,
					exit = {p}5,
					status = {p}6
				where id = {p}7;
			"
		);
		let finished_at = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = db::params![
			error.map(db::value::Json),
			finished_at,
			log,
			output.map(db::value::Json),
			exit.map(db::value::Json),
			status,
			id
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		// Publish the status message.
		tokio::spawn({
			let server = self.clone();
			let id = id.clone();
			async move {
				server
					.messenger
					.publish(format!("processes.{id}.status"), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		let output = tg::process::finish::Output { finished: true };
		Ok(output)
	}

	async fn verify_checksum(
		&self,
		parent_process_id: tg::process::Id,
		value: &tg::Value,
		expected: &tg::Checksum,
	) -> tg::Result<()> {
		if matches!(expected, tg::Checksum::Any) {
			return Ok(());
		}

		// Get the checksum.
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
		let command = tg::Command::builder(host).args(args).build();
		let arg = tg::process::spawn::Arg {
			create: false,
			command: Some(command.id(self).await?),
			parent: Some(parent_process_id),
			..Default::default()
		};
		let output = tg::Process::run(self, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to compute the checksum"))?;

		// Parse the checksum.
		let checksum = output
			.try_unwrap_string()
			.map_err(|_| tg::error!("expected a string"))?;
		let checksum = checksum
			.parse::<tg::Checksum>()
			.map_err(|_| tg::error!(%checksum, "failed to parse checksum string"))?;

		// Compare the checksums.
		if matches!(expected, tg::Checksum::None) {
			return Err(tg::error!("no checksum provided, actual {checksum}"));
		} else if &checksum != expected {
			return Err(tg::error!(
				"checksums do not match, expected {expected}, actual {checksum}"
			));
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_finish_process_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.try_finish_process(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
