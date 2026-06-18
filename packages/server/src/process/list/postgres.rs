use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn list_processes_postgres(
		&self,
		process_store: &db::postgres::Database,
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		let Some(principal) = principal else {
			return Ok(Vec::new());
		};

		let connection = process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;

		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::process::data::Child>>>")]
			children: Option<Vec<tg::process::data::Child>>,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			command: tg::command::Id,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Debug>>")]
			debug: Option<tg::process::Debug>,
			error: Option<String>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: String,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			log: Option<tg::blob::Id>,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			retry: bool,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			sandbox: tg::sandbox::Id,
			started_at: Option<i64>,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			status: tg::process::Status,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stderr: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdin: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdout: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Tty>>")]
			tty: Option<tg::process::Tty>,
		}
		let creator_condition = if matches!(principal, tg::Principal::Root) {
			"creator is null"
		} else {
			"creator = $1"
		};
		let statement = formatdoc!(
			"
				select
					processes.id,
					actual_checksum,
					cacheable,
					(select coalesce(json_agg(json_build_object('cached', cached, 'process', child, 'options', options::json) order by position), '[]'::json) from process_children where process = processes.id) as children,
					command,
					created_at,
					debug,
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
				where status != 'finished' and {creator_condition};
			"
		);
		let principal_string = principal.to_string();
		let rows = if matches!(principal, tg::Principal::Root) {
			connection.inner().query(&statement, &[]).await
		} else {
			connection
				.inner()
				.query(&statement, &[&principal_string])
				.await
		}
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let outputs = rows
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|error| tg::error!(!error, "failed to deserialize the row"))
			})
			.map(|row| {
				let row = row?;
				let error = row
					.error
					.map(|s| {
						if s.starts_with('{') {
							serde_json::from_str(&s)
								.map(tg::Either::Left)
								.map_err(|error| {
									tg::error!(!error, "failed to deserialize the error")
								})
						} else {
							s.parse()
								.map(tg::Either::Left)
								.map(tg::Either::Right)
								.map_err(|error| tg::error!(!error, "failed to parse the error id"))
						}
					})
					.transpose()?;
				let data = tg::process::Data {
					actual_checksum: row.actual_checksum,
					cacheable: row.cacheable,
					children: row.children,
					command: row.command,
					created_at: row.created_at,
					debug: row.debug,
					error,
					exit: row.exit,
					expected_checksum: row.expected_checksum,
					finished_at: row.finished_at,
					host: row.host,
					log: row.log.map(tg::Either::Left),
					output: row.output,
					retry: row.retry,
					sandbox: row.sandbox,
					started_at: row.started_at,
					status: row.status,
					stderr: row.stderr.unwrap_or(tg::process::Stdio::Null),
					stdin: row.stdin.unwrap_or(tg::process::Stdio::Null),
					stdout: row.stdout.unwrap_or(tg::process::Stdio::Null),
					tty: row.tty,
				};
				let output = tg::process::get::Output {
					id: row.id,
					data,
					location: None,
					metadata: None,
				};
				Ok(output)
			})
			.collect::<tg::Result<_>>()?;

		Ok(outputs)
	}
}
