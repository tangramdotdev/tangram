use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the process.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			id: Option<tg::process::Id>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			#[tangram_database(as = "Vec<db::postgres::value::FromStr>")]
			children: Vec<tg::Referent<tg::process::Id>>,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			command: tg::command::Id,
			created_at: i64,
			dequeued_at: Option<i64>,
			enqueued_at: Option<i64>,
			error: Option<String>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: String,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			log: Option<tg::blob::Id>,
			#[tangram_database(as = "Option<db::value::Json<Vec<tg::process::data::Mount>>>")]
			mounts: Option<Vec<tg::process::data::Mount>>,
			network: bool,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			retry: bool,
			started_at: Option<i64>,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			status: tg::process::Status,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stderr: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdin: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			stdout: Option<tg::process::Stdio>,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					actual_checksum,
					cacheable,
					(select coalesce(array_agg(child), '{}') from process_children where process = ids.id) as children,
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
				let id = row.id.ok_or_else(|| tg::error!("expected id"))?;
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
					cacheable: row.cacheable,
					children: Some(row.children),
					command: row.command,
					created_at: row.created_at,
					dequeued_at: row.dequeued_at,
					enqueued_at: row.enqueued_at,
					error,
					exit: row.exit,
					expected_checksum: row.expected_checksum,
					finished_at: row.finished_at,
					host: row.host,
					log: row.log,
					output: row.output,
					retry: row.retry,
					mounts: row.mounts.unwrap_or_default(),
					network: row.network,
					started_at: row.started_at,
					status: row.status,
					stderr: row.stderr,
					stdin: row.stdin,
					stdout: row.stdout,
				};
				let output = tg::process::get::Output { id, data };
				Ok(Some(output))
			})
			.collect::<tg::Result<_>>()?;

		Ok(outputs)
	}
}
