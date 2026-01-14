use {
	super::Index,
	crate::{ObjectStored, ProcessStored},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	time,
};

impl Index {
	pub async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let id = id.clone();
		let output = connection
			.with(move |connection, cache| {
				try_touch_object_and_get_stored_and_metadata_sync(
					connection, cache, &id, touched_at,
				)
			})
			.await?;
		Ok(output)
	}

	pub async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection, cache| {
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
					let output = try_touch_object_and_get_stored_and_metadata_batch_sync(
						&transaction,
						cache,
						&ids,
						touched_at,
					)?;
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;
					Ok::<_, tg::Error>(output)
				}
			})
			.await?;
		Ok(output)
	}

	pub async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let id = id.clone();
		let output = connection
			.with(move |connection, cache| {
				try_touch_process_and_get_stored_and_metadata_sync(
					connection, cache, &id, touched_at,
				)
			})
			.await?;
		Ok(output)
	}

	pub async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let ids = ids.to_owned();
		let output = connection
			.with(move |connection, cache| {
				try_touch_process_and_get_stored_and_metadata_batch_sync(
					connection, cache, &ids, touched_at,
				)
			})
			.await?;
		Ok(output)
	}

	pub async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let id = id.clone();
		let n = connection
			.with(move |connection, cache| {
				let statement = indoc!(
					"
						update objects
						set touched_at = max(touched_at, ?1)
						where id = ?2;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let n = statement
					.execute(sqlite::params![touched_at, id.to_bytes().to_vec()])
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				Ok::<_, tg::Error>(n)
			})
			.await?;
		if n == 0 {
			return Err(tg::error!("failed to find the object"));
		}
		Ok(())
	}

	pub async fn touch_process(&self, id: &tg::process::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let id = id.clone();
		let n = connection
			.with(move |connection, cache| {
				let statement = indoc!(
					"
						update processes
						set touched_at = max(touched_at, ?1)
						where id = ?2;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let n = statement
					.execute(sqlite::params![touched_at, id.to_bytes().to_vec()])
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				Ok::<_, tg::Error>(n)
			})
			.await?;
		if n == 0 {
			return Err(tg::error!("failed to find the process"));
		}
		Ok(())
	}
}

fn try_touch_object_and_get_stored_and_metadata_sync(
	connection: &sqlite::Connection,
	cache: &db::sqlite::Cache,
	id: &tg::object::Id,
	touched_at: i64,
) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
	#[derive(db::sqlite::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_size: u64,
		node_solvable: bool,
		node_solved: bool,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_size: Option<u64>,
		subtree_solvable: Option<bool>,
		subtree_solved: Option<bool>,
		subtree_stored: bool,
	}
	let statement = indoc!(
		"
			update objects
			set touched_at = max(?1, touched_at)
			where id = ?2
			returning
				node_size,
				node_solvable,
				node_solved,
				subtree_count,
				subtree_depth,
				subtree_size,
				subtree_solvable,
				subtree_solved,
				subtree_stored;
		"
	);
	let mut statement = cache
		.get(connection, statement.into())
		.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
	let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
	let mut rows = statement
		.query(params)
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
	let Some(row) = rows
		.next()
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
	else {
		return Ok(None);
	};
	let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
		.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
	let stored = ObjectStored {
		subtree: row.subtree_stored,
	};
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: row.node_size,
			solvable: row.node_solvable,
			solved: row.node_solved,
		},
		subtree: tg::object::metadata::Subtree {
			count: row.subtree_count,
			depth: row.subtree_depth,
			size: row.subtree_size,
			solvable: row.subtree_solvable,
			solved: row.subtree_solved,
		},
	};
	Ok(Some((stored, metadata)))
}

fn try_touch_object_and_get_stored_and_metadata_batch_sync(
	transaction: &sqlite::Transaction,
	cache: &db::sqlite::Cache,
	ids: &[tg::object::Id],
	touched_at: i64,
) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
	if ids.is_empty() {
		return Ok(vec![]);
	}
	#[derive(db::sqlite::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_size: u64,
		node_solvable: bool,
		node_solved: bool,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_size: Option<u64>,
		subtree_solvable: Option<bool>,
		subtree_solved: Option<bool>,
		subtree_stored: bool,
	}
	let statement = indoc!(
		"
			update objects
			set touched_at = max(?1, touched_at)
			where id = ?2
			returning
				node_size,
				node_solvable,
				node_solved,
				subtree_count,
				subtree_depth,
				subtree_size,
				subtree_solvable,
				subtree_solved,
				subtree_stored;
		"
	);
	let mut statement = cache
		.get(transaction, statement.into())
		.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
	let mut outputs = Vec::new();
	for id in ids {
		let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			outputs.push(None);
			continue;
		};
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let stored = ObjectStored {
			subtree: row.subtree_stored,
		};
		let metadata = tg::object::Metadata {
			node: tg::object::metadata::Node {
				size: row.node_size,
				solvable: row.node_solvable,
				solved: row.node_solved,
			},
			subtree: tg::object::metadata::Subtree {
				count: row.subtree_count,
				depth: row.subtree_depth,
				size: row.subtree_size,
				solvable: row.subtree_solvable,
				solved: row.subtree_solved,
			},
		};
		outputs.push(Some((stored, metadata)));
	}
	Ok(outputs)
}

fn try_touch_process_and_get_stored_and_metadata_sync(
	connection: &sqlite::Connection,
	cache: &db::sqlite::Cache,
	id: &tg::process::Id,
	touched_at: i64,
) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
	#[derive(db::sqlite::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_command_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_error_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_log_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_output_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_command_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_error_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_log_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_output_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_count: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_stored: u64,
	}
	let statement = indoc!(
		"
			update processes
			set touched_at = max(?1, touched_at)
			where id = ?2
			returning
				node_command_count,
				node_command_depth,
				node_command_size,
				node_command_stored,
				node_log_count,
				node_log_depth,
				node_log_size,
				node_log_stored,
				node_output_count,
				node_output_depth,
				node_output_size,
				node_output_stored,
				subtree_command_count,
				subtree_command_depth,
				subtree_command_size,
				subtree_command_stored,
				subtree_log_count,
				subtree_log_depth,
				subtree_log_size,
				subtree_log_stored,
				subtree_output_count,
				subtree_output_depth,
				subtree_output_size,
				subtree_output_stored,
				subtree_count,
				subtree_stored;
		"
	);
	let mut statement = cache
		.get(connection, statement.into())
		.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
	let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
	let mut rows = statement
		.query(params)
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
	let Some(row) = rows
		.next()
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
	else {
		return Ok(None);
	};
	let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
		.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
	let stored = ProcessStored {
		node_command: row.node_command_stored != 0,
		node_error: row.node_error_stored != 0,
		node_log: row.node_log_stored != 0,
		node_output: row.node_output_stored != 0,
		subtree: row.subtree_stored != 0,
		subtree_command: row.subtree_command_stored != 0,
		subtree_error: row.subtree_error_stored != 0,
		subtree_log: row.subtree_log_stored != 0,
		subtree_output: row.subtree_output_stored != 0,
	};
	let node = tg::process::metadata::Node {
		command: tg::object::metadata::Subtree {
			count: row.node_command_count,
			depth: row.node_command_depth,
			size: row.node_command_size,
			solvable: None,
			solved: None,
		},
		error: tg::object::metadata::Subtree {
			count: row.node_error_count,
			depth: row.node_error_depth,
			size: row.node_error_size,
			solvable: None,
			solved: None,
		},
		log: tg::object::metadata::Subtree {
			count: row.node_log_count,
			depth: row.node_log_depth,
			size: row.node_log_size,
			solvable: None,
			solved: None,
		},
		output: tg::object::metadata::Subtree {
			count: row.node_output_count,
			depth: row.node_output_depth,
			size: row.node_output_size,
			solvable: None,
			solved: None,
		},
	};
	let subtree = tg::process::metadata::Subtree {
		command: tg::object::metadata::Subtree {
			count: row.subtree_command_count,
			depth: row.subtree_command_depth,
			size: row.subtree_command_size,
			solvable: None,
			solved: None,
		},
		error: tg::object::metadata::Subtree {
			count: row.subtree_error_count,
			depth: row.subtree_error_depth,
			size: row.subtree_error_size,
			solvable: None,
			solved: None,
		},
		log: tg::object::metadata::Subtree {
			count: row.subtree_log_count,
			depth: row.subtree_log_depth,
			size: row.subtree_log_size,
			solvable: None,
			solved: None,
		},
		output: tg::object::metadata::Subtree {
			count: row.subtree_output_count,
			depth: row.subtree_output_depth,
			size: row.subtree_output_size,
			solvable: None,
			solved: None,
		},
		count: row.subtree_count,
	};
	let metadata = tg::process::Metadata { node, subtree };
	Ok(Some((stored, metadata)))
}

fn try_touch_process_and_get_stored_and_metadata_batch_sync(
	connection: &sqlite::Connection,
	cache: &db::sqlite::Cache,
	ids: &[tg::process::Id],
	touched_at: i64,
) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
	if ids.is_empty() {
		return Ok(vec![]);
	}
	#[derive(db::sqlite::row::Deserialize)]
	struct Row {
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_command_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_command_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_error_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_error_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_log_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_log_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		node_output_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		node_output_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_command_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_command_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_error_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_error_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_log_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_log_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_count: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_depth: Option<u64>,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_output_size: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_output_stored: u64,
		#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
		subtree_count: Option<u64>,
		#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
		subtree_stored: u64,
	}
	let statement = indoc!(
		"
			update processes
			set touched_at = max(?1, touched_at)
			where id = ?2
			returning
				node_command_count,
				node_command_depth,
				node_command_size,
				node_command_stored,
				node_error_count,
				node_error_depth,
				node_error_size,
				node_error_stored,
				node_log_count,
				node_log_depth,
				node_log_size,
				node_log_stored,
				node_output_count,
				node_output_depth,
				node_output_size,
				node_output_stored,
				subtree_command_count,
				subtree_command_depth,
				subtree_command_size,
				subtree_command_stored,
				subtree_error_count,
				subtree_error_depth,
				subtree_error_size,
				subtree_error_stored,
				subtree_log_count,
				subtree_log_depth,
				subtree_log_size,
				subtree_log_stored,
				subtree_output_count,
				subtree_output_depth,
				subtree_output_size,
				subtree_output_stored,
				subtree_count,
				subtree_stored;
		"
	);
	let mut statement = cache
		.get(connection, statement.into())
		.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
	let mut outputs = Vec::new();
	for id in ids {
		let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			outputs.push(None);
			continue;
		};
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let stored = ProcessStored {
			node_command: row.node_command_stored != 0,
			node_error: row.node_error_stored != 0,
			node_log: row.node_log_stored != 0,
			node_output: row.node_output_stored != 0,
			subtree: row.subtree_stored != 0,
			subtree_command: row.subtree_command_stored != 0,
			subtree_error: row.subtree_error_stored != 0,
			subtree_log: row.subtree_log_stored != 0,
			subtree_output: row.subtree_output_stored != 0,
		};
		let node = tg::process::metadata::Node {
			command: tg::object::metadata::Subtree {
				count: row.node_command_count,
				depth: row.node_command_depth,
				size: row.node_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: row.node_error_count,
				depth: row.node_error_depth,
				size: row.node_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: row.node_log_count,
				depth: row.node_log_depth,
				size: row.node_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: row.node_output_count,
				depth: row.node_output_depth,
				size: row.node_output_size,
				solvable: None,
				solved: None,
			},
		};
		let subtree = tg::process::metadata::Subtree {
			command: tg::object::metadata::Subtree {
				count: row.subtree_command_count,
				depth: row.subtree_command_depth,
				size: row.subtree_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: row.subtree_error_count,
				depth: row.subtree_error_depth,
				size: row.subtree_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: row.subtree_log_count,
				depth: row.subtree_log_depth,
				size: row.subtree_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: row.subtree_output_count,
				depth: row.subtree_output_depth,
				size: row.subtree_output_size,
				solvable: None,
				solved: None,
			},
			count: row.subtree_count,
		};
		let metadata = tg::process::Metadata { node, subtree };
		outputs.push(Some((stored, metadata)));
	}
	Ok(outputs)
}
