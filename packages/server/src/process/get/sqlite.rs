use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let outputs = connection
			.with({
				let ids = ids.to_owned();
				move |connection, cache| {
					Self::try_get_process_batch_sqlite_sync(connection, cache, &ids)
				}
			})
			.await?;

		Ok(outputs)
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
			dequeued_at: Option<i64>,
			enqueued_at: Option<i64>,
			error: Option<String>,
			exit: Option<u8>,
			expected_checksum: Option<String>,
			finished_at: Option<i64>,
			host: String,
			log: Option<String>,
			output: Option<String>,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			retry: u64,
			mounts: Option<String>,
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			network: u64,
			started_at: Option<i64>,
			status: String,
			stderr: Option<String>,
			stdin: Option<String>,
			stdout: Option<String>,
		}
		let statement = indoc!(
			"
				select
					actual_checksum,
					cacheable,
					command,
					created_at,
					dequeued_at,
					enqueued_at,
					error,
					exit,
					expected_checksum,
					finished_at,
					host,
					log,
					output,
					retry,
					mounts,
					network,
					started_at,
					status,
					stderr,
					stdin,
					stdout
				from processes
				where id = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Deserialize the row.
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let actual_checksum =
			row.actual_checksum.map(|s| s.parse()).transpose().map_err(
				|source| tg::error!(!source, %id, "failed to parse the actual checksum"),
			)?;
		let cacheable = row.cacheable != 0;
		let command = row
			.command
			.parse()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the command"))?;
		let error = row.error.map(|s| {
			if s.starts_with('{') {
				tg::Either::Left(serde_json::from_str(&s).unwrap())
			} else {
				tg::Either::Right(s.parse().unwrap())
			}
		});
		let expected_checksum = row
			.expected_checksum
			.map(|s| s.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the expected checksum"))?;
		let log = row
			.log
			.map(|s| s.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the log id"))?;
		let output = row
			.output
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
		let retry = row.retry != 0;
		let mounts = row
			.mounts
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?
			.unwrap_or_default();
		let network = row.network != 0;
		let status = row
			.status
			.parse()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the status"))?;
		let stderr = row
			.stderr
			.map(|s| s.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the stderr pipe"))?;
		let stdin = row
			.stdin
			.map(|s| s.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the stdin pipe"))?;
		let stdout = row
			.stdout
			.map(|s| s.parse())
			.transpose()
			.map_err(|source| tg::error!(!source, %id, "failed to parse the stdout pipe"))?;

		// Get the children.
		#[derive(db::sqlite::row::Deserialize)]
		struct ChildRow {
			child: String,
			options: db::value::Json<tg::referent::Options>,
		}
		let statement = indoc!(
			"
				select child, options
				from process_children
				where process = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut child_rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut children = Vec::new();
		while let Some(child_row) = child_rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let child_row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(child_row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let item = child_row
				.child
				.parse()
				.map_err(|source| tg::error!(!source, %id, "failed to parse the child id"))?;
			let options = child_row.options.0;
			let referent = tg::Referent { item, options };
			children.push(referent);
		}

		let data = tg::process::Data {
			actual_checksum,
			cacheable,
			children: Some(children),
			command,
			created_at: row.created_at,
			dequeued_at: row.dequeued_at,
			enqueued_at: row.enqueued_at,
			error,
			exit: row.exit,
			expected_checksum,
			finished_at: row.finished_at,
			host: row.host,
			log,
			output,
			retry,
			mounts,
			network,
			started_at: row.started_at,
			status,
			stderr,
			stdin,
			stdout,
		};

		let output = tg::process::get::Output {
			id: id.clone(),
			data,
		};

		Ok(Some(output))
	}
}
