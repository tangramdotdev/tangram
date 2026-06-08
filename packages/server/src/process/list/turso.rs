use {
	crate::Session,
	indoc::{formatdoc, indoc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn list_processes_turso(
		&self,
		process_store: &db::turso::Database,
		principal: Option<&tg::Principal>,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		if principal.is_none() {
			return Ok(Vec::new());
		}

		let connection = process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			#[tangram_database(as = "db::value::FromStr")]
			command: tg::command::Id,
			created_at: i64,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Debug>>")]
			debug: Option<tg::process::Debug>,
			error: Option<String>,
			exit: Option<u8>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			expected_checksum: Option<tg::Checksum>,
			finished_at: Option<i64>,
			host: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			log: Option<tg::blob::Id>,
			#[tangram_database(as = "Option<db::value::Json<tg::value::Data>>")]
			output: Option<tg::value::Data>,
			retry: bool,
			#[tangram_database(as = "db::value::FromStr")]
			sandbox: tg::sandbox::Id,
			started_at: Option<i64>,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::process::Status,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			stderr: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			stdin: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			stdout: Option<tg::process::Stdio>,
			#[tangram_database(as = "Option<db::value::Json<tg::process::Tty>>")]
			tty: Option<tg::process::Tty>,
		}
		let creator_condition = if matches!(principal, Some(tg::Principal::Root)) {
			"creator is null"
		} else {
			"creator = ?1"
		};
		let statement = formatdoc!(
			"
				select
					id,
					actual_checksum,
					cacheable,
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
		let rows = if matches!(principal, Some(tg::Principal::Root)) {
			connection
				.query_all_into::<Row>(statement.into(), db::params![])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		} else {
			connection
				.query_all_into::<Row>(
					statement.into(),
					db::params![principal.unwrap().to_string()],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};

		let mut outputs = Vec::new();
		for row in rows {
			#[derive(db::row::Deserialize)]
			struct ChildRow {
				cached: bool,
				#[tangram_database(as = "db::value::FromStr")]
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
			let child_rows = connection
				.query_all_into::<ChildRow>(statement.into(), db::params![row.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let children = child_rows
				.into_iter()
				.map(|child_row| tg::process::data::Child {
					cached: child_row.cached,
					process: child_row.child,
					options: child_row.options.0,
				})
				.collect();

			let error = row
				.error
				.map(|s| {
					if s.starts_with('{') {
						serde_json::from_str(&s)
							.map(tg::Either::Left)
							.map_err(|error| tg::error!(!error, "failed to deserialize the error"))
					} else {
						s.parse()
							.map(tg::Either::Right)
							.map_err(|error| tg::error!(!error, "failed to parse the error id"))
					}
				})
				.transpose()?;
			let data = tg::process::Data {
				actual_checksum: row.actual_checksum,
				cacheable: row.cacheable,
				children: Some(children),
				command: row.command,
				created_at: row.created_at,
				debug: row.debug,
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
