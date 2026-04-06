use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_batch_postgres(
		&self,
		register: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		// Get a register connection.
		let connection = register
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a register connection"))?;

		// Get the process.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			id: Option<tg::process::Id>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: Option<bool>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::process::data::Child>>>")]
			children: Option<Vec<tg::process::data::Child>>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			command: Option<tg::command::Id>,
			created_at: Option<i64>,
			error: Option<String>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: Option<String>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			log: Option<tg::blob::Id>,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			retry: Option<bool>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			sandbox: Option<tg::sandbox::Id>,
			started_at: Option<i64>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			status: Option<tg::process::Status>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stderr: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdin: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdout: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Tty>>")]
			tty: Option<tg::process::Tty>,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					actual_checksum,
					cacheable,
					(select coalesce(json_agg(json_build_object('cached', cached, 'process', child, 'options', options::json) order by position), '[]'::json) from process_children where process = ids.id) as children,
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
				from unnest($1::text[]) as ids (id)
				left join processes on processes.id = ids.id;
			"
		);
		let outputs = connection
			.inner()
			.query(
				statement,
				&[&ids.iter().map(ToString::to_string).collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map(|row| {
				let row = row?;
				// If id is None, the process does not exist in the register.
				let Some(id) = row.id else {
					return Ok(None);
				};
				// Unwrap required fields. These should always be present if the process exists.
				let cacheable = row
					.cacheable
					.ok_or_else(|| tg::error!(%id, "missing cacheable field"))?;
				let children = row
					.children
					.ok_or_else(|| tg::error!(%id, "missing children field"))?;
				let command = row
					.command
					.ok_or_else(|| tg::error!(%id, "missing command field"))?;
				let created_at = row
					.created_at
					.ok_or_else(|| tg::error!(%id, "missing created_at field"))?;
				let host = row
					.host
					.ok_or_else(|| tg::error!(%id, "missing host field"))?;
				let retry = row
					.retry
					.ok_or_else(|| tg::error!(%id, "missing retry field"))?;
				let status = row
					.status
					.ok_or_else(|| tg::error!(%id, "missing status field"))?;
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
							s.parse().map(tg::Either::Right).map_err(|source| {
								tg::error!(!source, "failed to parse the error id")
							})
						}
					})
					.transpose()?;
				let data = tg::process::Data {
					actual_checksum: row.actual_checksum,
					cacheable,
					children: Some(children),
					command,
					created_at,
					error,
					exit: row.exit,
					expected_checksum: row.expected_checksum,
					finished_at: row.finished_at,
					host,
					log: row.log,
					output: row.output,
					retry,
					sandbox: row.sandbox,
					started_at: row.started_at,
					status,
					stderr: row.stderr.unwrap_or(tg::process::Stdio::Null),
					stdin: row.stdin.unwrap_or(tg::process::Stdio::Null),
					stdout: row.stdout.unwrap_or(tg::process::Stdio::Null),
					tty: row.tty,
				};
				let output = tg::process::get::Output {
					id,
					data,
					metadata: None,
				};
				Ok(Some(output))
			})
			.collect::<tg::Result<_>>()?;

		Ok(outputs)
	}
}
