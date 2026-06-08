use {
	crate::{Session, authorization},
	indoc::{formatdoc, indoc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_process_batch_turso(
		&self,
		process_store: &db::turso::Database,
		ids: &[tg::process::Id],
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let mut connection = process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			outputs.push(Self::try_get_process_turso(&transaction, id, principal, now).await?);
		}
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(outputs)
	}

	async fn try_get_process_turso(
		connection: &impl db::Query<Error = db::turso::Error>,
		id: &tg::process::Id,
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<Option<tg::process::get::Output>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			actual_checksum: Option<String>,
			cacheable: bool,
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
			retry: bool,
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
		let row_future = async {
			connection
				.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))
		};
		let grant_future = Self::try_get_process_grants_turso(connection, id, principal, now);
		let (row, grants) = futures::try_join!(row_future, grant_future)?;
		let Some(row) = row else {
			return Ok(None);
		};

		let actual_checksum = row
			.actual_checksum
			.map(|s| s.parse())
			.transpose()
			.map_err(|error| tg::error!(!error, %id, "failed to parse the actual checksum"))?;
		let cacheable = row.cacheable;
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
		let retry = row.retry;
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

		#[derive(db::row::Deserialize)]
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
		let child_rows = connection
			.query_all_into::<ChildRow>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut children = Vec::new();
		for child_row in child_rows {
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

		if Self::authorize_process(id, principal, &grants) {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	async fn try_get_process_grants_turso(
		connection: &impl db::Query<Error = db::turso::Error>,
		id: &tg::process::Id,
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<Vec<authorization::ProcessGrant>> {
		let Some(principal) = principal else {
			return Ok(Vec::new());
		};
		if matches!(principal, tg::Principal::Root) {
			return Ok(Vec::new());
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			expires_at: i64,
			node: bool,
			node_command: bool,
			node_error: bool,
			node_log: bool,
			node_output: bool,
			principal: String,
			process: String,
			subtree: bool,
			subtree_command: bool,
			subtree_error: bool,
			subtree_log: bool,
			subtree_output: bool,
		}
		let statement = indoc!(
			"
				select
					created_at,
					expires_at,
					node,
					node_command,
					node_error,
					node_log,
					node_output,
					principal,
					process,
					subtree,
					subtree_command,
					subtree_error,
					subtree_log,
					subtree_output
				from process_grants
				where process = ?1
					and principal = ?2
					and expires_at > ?3;
			"
		);
		let rows = connection
			.query_all_into::<Row>(
				statement.into(),
				db::params![id.to_string(), principal.to_string(), now],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let grants = rows
			.into_iter()
			.map(|row| {
				Ok(authorization::ProcessGrant {
					created_at: row.created_at,
					expires_at: row.expires_at,
					node: row.node,
					node_command: row.node_command,
					node_error: row.node_error,
					node_log: row.node_log,
					node_output: row.node_output,
					principal: row
						.principal
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the principal"))?,
					process: row
						.process
						.parse()
						.map_err(|error| tg::error!(!error, "failed to parse the process"))?,
					subtree: row.subtree,
					subtree_command: row.subtree_command,
					subtree_error: row.subtree_error,
					subtree_log: row.subtree_log,
					subtree_output: row.subtree_output,
				})
			})
			.collect::<tg::Result<_>>()?;
		Ok(grants)
	}
}
