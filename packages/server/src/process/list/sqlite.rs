use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn list_processes_sqlite(
		&self,
		process_store: &db::sqlite::Database,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		let connection = process_store
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		let outputs = connection
			.with(move |connection, cache| Self::list_processes_sqlite_sync(connection, cache))
			.await?;

		Ok(outputs)
	}

	pub(crate) fn list_processes_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::sqlite::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			#[tangram_database(as = "db::sqlite::value::FromStr")]
			command: tg::command::Id,
			created_at: i64,
			error: Option<String>,
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: String,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			log: Option<tg::blob::Id>,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			retry: bool,
			#[tangram_database(as = "db::sqlite::value::FromStr")]
			sandbox: tg::sandbox::Id,
			started_at: Option<i64>,
			#[tangram_database(as = "db::sqlite::value::FromStr")]
			status: tg::process::Status,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			stderr: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			stdin: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::sqlite::value::FromStr>")]
			stdout: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Tty>>")]
			tty: Option<tg::process::Tty>,
		}
		let statement = indoc!(
			"
				select
					id,
					actual_checksum,
					cacheable,
					command,
					created_at,
					error,
					exit,
					expected_checksum,
					finished_at,
					host,
					log,
					output,
					retry,
					sandbox,
					started_at,
					status,
					stderr,
					stdin,
					stdout,
					tty
				from processes
				where status != 'finished';
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let mut outputs = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;

			// Get the children for this process.
			#[derive(db::sqlite::row::Deserialize)]
			struct ChildRow {
				cached: bool,
				#[tangram_database(as = "db::sqlite::value::FromStr")]
				child: tg::process::Id,
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
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let mut child_rows = statement
				.query([&row.id.to_string()])
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let mut children = Vec::new();
			while let Some(child_row) = child_rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			{
				let child_row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(child_row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let child = tg::process::data::Child {
					cached: child_row.cached,
					process: child_row.child,
					options: child_row.options.0,
				};
				children.push(child);
			}

			let error = row
				.error
				.map(|s| {
					if s.starts_with('{') {
						serde_json::from_str(&s)
							.map(tg::Either::Left)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the error")
							})
					} else {
						s.parse()
							.map(tg::Either::Right)
							.map_err(|source| tg::error!(!source, "failed to parse the error id"))
					}
				})
				.transpose()?;
			let data = tg::process::Data {
				actual_checksum: row.actual_checksum,
				cacheable: row.cacheable,
				children: Some(children),
				command: row.command,
				created_at: row.created_at,
				error,
				exit: row.exit,
				expected_checksum: row.expected_checksum,
				finished_at: row.finished_at,
				host: row.host,
				log: row.log,
				output: row.output,
				retry: row.retry,
				sandbox: row.sandbox,
				started_at: row.started_at,
				status: row.status,
				stderr: row.stderr.unwrap_or_default(),
				stdin: row.stdin.unwrap_or_default(),
				stdout: row.stdout.unwrap_or_default(),
				tty: row.tty,
			};

			let output = tg::process::get::Output {
				id: row.id,
				data,
				location: None,
				metadata: None,
			};
			outputs.push(output);
		}

		Ok(outputs)
	}
}
