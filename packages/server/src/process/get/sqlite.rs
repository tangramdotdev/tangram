use {
	crate::Session,
	indoc::{formatdoc, indoc},
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_process_batch_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let mut connection = process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		let outputs = transaction
			.with({
				let ids = ids.to_owned();
				move |connection, cache| {
					Self::try_get_process_batch_sqlite_sync(connection, cache, &ids)
				}
			})
			.await?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		let mut authorized_outputs = Vec::with_capacity(ids.len());
		for (id, output) in ids.iter().zip(outputs) {
			let output = if let Some(mut output) = output {
				let resource = tg::grant::Resource::Id(id.clone().into());
				let permission = tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::Node,
				);
				if self.authorize(resource, permission).await? == Some(true) {
					output.location = Some(self.server.config().region.clone().map_or_else(
						|| tg::Location::Local(tg::location::Local::default()),
						|region| {
							tg::Location::Local(tg::location::Local {
								region: Some(region),
							})
						},
					));
					Some(output)
				} else {
					None
				}
			} else {
				None
			};
			authorized_outputs.push(output);
		}

		Ok(authorized_outputs)
	}

	pub(crate) fn try_get_process_batch_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			outputs.push(Self::try_get_process_sqlite_sync(connection, cache, id)?);
		}
		Ok(outputs)
	}

	pub(crate) fn try_get_process_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Get the process.
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			actual_checksum: Option<String>,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			cacheable: u64,
			command: String,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Debug>>")]
			debug: Option<tg::process::Debug>,
			error: Option<String>,
			exit: Option<u8>,
			expected_checksum: Option<String>,
			finished_at: Option<i64>,
			host: String,
			log: Option<String>,
			output: Option<String>,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			retry: u64,
			sandbox: String,
			started_at: Option<i64>,
			status: String,
			stderr: Option<String>,
			stdin: Option<String>,
			stdout: Option<String>,
			tty: Option<String>,
		}
		let statement = formatdoc!(
			"
				select
					processes.actual_checksum,
					processes.cacheable,
					processes.command,
					processes.created_at,
					processes.debug,
					processes.error,
					processes.exit,
					processes.expected_checksum,
					processes.finished_at,
					processes.host,
					processes.log,
					processes.output,
					processes.retry,
					processes.sandbox,
					processes.started_at,
					processes.status,
					processes.stderr,
					processes.stdin,
					processes.stdout,
					processes.tty
				from processes
				where processes.id = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Deserialize the row.
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
		let actual_checksum = row
			.actual_checksum
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the actual checksum"))?;
		let cacheable = row.cacheable != 0;
		let command = row
			.command
			.parse()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the command"))?;
		let error = row
			.error
			.map(|s| {
				if s.starts_with('{') {
					serde_json::from_str(&s)
						.map(tg::Either::Left)
						.map_err(|error| tg::error!(!error, %id, "failed to deserialize the error"))
				} else {
					s.parse()
						.map(tg::Either::Right)
						.map_err(|error| tg::error!(!error, %id, "failed to parse the error id"))
				}
			})
			.transpose()?;
		let expected_checksum = row
			.expected_checksum
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the expected checksum"))?;
		let log = row
			.log
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the log id"))?;
		let output = row
			.output
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize"))?;
		let retry = row.retry != 0;
		let sandbox = row
			.sandbox
			.parse()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the sandbox"))?;
		let status = row
			.status
			.parse()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the status"))?;
		let stderr = row
			.stderr
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the stderr pipe"))?;
		let stdin = row
			.stdin
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the stdin pipe"))?;
		let stdout = row
			.stdout
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the stdout pipe"))?;
		let tty = row
			.tty
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the tty"))?;

		// Get the children.
		#[derive(db::sqlite::row::Deserialize)]
		struct ChildRow {
			cached: bool,
			child: String,
			options: db::value::Json<tg::referent::Options>,
		}
		let statement = indoc!(
			"
				select cached, child, options
				from process_children
				where process = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut child_rows = statement
			.query([id.to_string()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut children = Vec::new();
		while let Some(child_row) = child_rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		{
			let child_row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(child_row)
				.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
			let cached = child_row.cached;
			let process = child_row
				.child
				.parse()
				.map_err(|error| tg::error!(!error, %id, "failed to parse the child id"))?;
			let options = child_row.options.0;
			children.push(tg::process::data::Child {
				cached,
				process,
				options,
			});
		}

		let data = tg::process::Data {
			actual_checksum,
			cacheable,
			children: Some(children),
			command,
			created_at: row.created_at,
			debug: row.debug,
			error,
			exit: row.exit,
			expected_checksum,
			finished_at: row.finished_at,
			host: row.host,
			log,
			output,
			retry,
			sandbox,
			started_at: row.started_at,
			status,
			stderr: stderr.unwrap_or(tg::process::Stdio::Null),
			stdin: stdin.unwrap_or(tg::process::Stdio::Null),
			stdout: stdout.unwrap_or(tg::process::Stdio::Null),
			tty,
		};

		let output = tg::process::get::Output {
			id: id.clone(),
			data,
			location: None,
			metadata: None,
		};

		Ok(Some(output))
	}
}
