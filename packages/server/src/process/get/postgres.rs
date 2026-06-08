use {
	crate::{Session, authorization},
	indoc::{formatdoc, indoc},
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_get_process_batch_postgres(
		&self,
		process_store: &db::postgres::Database,
		ids: &[tg::process::Id],
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		// Get a process store connection.
		let mut connection = process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;

		// Get the processes.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::FromStr")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::FromStr>")]
			actual_checksum: Option<tg::Checksum>,
			cacheable: bool,
			#[tangram_database(as = "db::value::Json<Vec<tg::process::data::Child>>")]
			children: Vec<tg::process::data::Child>,
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
		let statement = formatdoc!(
			"
				select
					processes.id,
					processes.actual_checksum,
					processes.cacheable,
					(select coalesce(json_agg(json_build_object('cached', cached, 'process', child, 'options', options::json) order by position), '[]'::json) from process_children where process = processes.id) as children,
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
				where processes.id = any($1::text[]);
			"
		);
		let id_strings = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
		let row_future = async {
			transaction
				.inner()
				.query(&statement, &[&id_strings])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))
		};
		let grant_future =
			Self::try_get_process_grants_batch_postgres(transaction.inner(), ids, principal, now);
		let (rows, grants) = futures::try_join!(row_future, grant_future)?;
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
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
								.map(tg::Either::Right)
								.map_err(|error| tg::error!(!error, "failed to parse the error id"))
						}
					})
					.transpose()?;
				let data = tg::process::Data {
					actual_checksum: row.actual_checksum,
					cacheable: row.cacheable,
					children: Some(row.children),
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
					stderr: row.stderr.unwrap_or(tg::process::Stdio::Null),
					stdin: row.stdin.unwrap_or(tg::process::Stdio::Null),
					stdout: row.stdout.unwrap_or(tg::process::Stdio::Null),
					tty: row.tty,
				};
				let output = tg::process::get::Output {
					id: row.id.clone(),
					data,
					location: None,
					metadata: None,
				};
				Ok((row.id, output))
			})
			.collect::<tg::Result<HashMap<_, _>>>()?;

		let outputs = ids
			.iter()
			.map(|id| {
				let output = outputs.get(id).cloned();
				let grants = grants.get(id).map(Vec::as_slice).unwrap_or_default();
				output.filter(|_| Self::authorize_process(id, principal, grants))
			})
			.collect();

		Ok(outputs)
	}

	async fn try_get_process_grants_batch_postgres(
		connection: &(impl tokio_postgres::GenericClient + Sync),
		ids: &[tg::process::Id],
		principal: Option<&tg::Principal>,
		now: i64,
	) -> tg::Result<HashMap<tg::process::Id, Vec<authorization::ProcessGrant>>> {
		let Some(principal) = principal else {
			return Ok(HashMap::default());
		};
		if matches!(principal, tg::Principal::Root) {
			return Ok(HashMap::default());
		}
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			created_at: i64,
			expires_at: i64,
			node: bool,
			node_command: bool,
			node_error: bool,
			node_log: bool,
			node_output: bool,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			principal: tg::Principal,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			process: tg::process::Id,
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
			where process = any($1::text[])
				and principal = $2
				and expires_at > $3;
		"
		);
		let id_strings = ids.iter().map(ToString::to_string).collect::<Vec<_>>();
		let principal = principal.to_string();
		let rows = connection
			.query(statement, &[&id_strings, &principal, &now])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut grants = HashMap::default();
		for row in &rows {
			let row = <Row as db::postgres::row::Deserialize>::deserialize(row)
				.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
			grants
				.entry(row.process.clone())
				.or_insert_with(Vec::new)
				.push(authorization::ProcessGrant {
					created_at: row.created_at,
					expires_at: row.expires_at,
					node: row.node,
					node_command: row.node_command,
					node_error: row.node_error,
					node_log: row.node_log,
					node_output: row.node_output,
					principal: row.principal,
					process: row.process,
					subtree: row.subtree,
					subtree_command: row.subtree_command,
					subtree_error: row.subtree_error,
					subtree_log: row.subtree_log,
					subtree_output: row.subtree_output,
				});
		}
		Ok(grants)
	}
}
