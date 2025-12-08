use {
	super::Output,
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_stored_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.clone();
				move |connection| Self::try_get_process_stored_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_stored_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					node_command_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_output_stored,
					subtree_stored
				from processes
				where id = ?1;
			",
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![id.to_bytes().to_vec()];
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
		let stored = Output {
			node_command: row.node_command_stored,
			node_output: row.node_output_stored,
			subtree_command: row.subtree_command_stored,
			subtree_output: row.subtree_output_stored,
			subtree: row.subtree_stored,
		};
		Ok(Some(stored))
	}

	pub(crate) async fn try_get_process_stored_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| Self::try_get_process_stored_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_stored_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					node_command_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_output_stored,
					subtree_stored
				from processes
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut outputs = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
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
			let stored = Output {
				node_command: row.node_command_stored,
				node_output: row.node_output_stored,
				subtree_command: row.subtree_command_stored,
				subtree_output: row.subtree_output_stored,
				subtree: row.subtree_stored,
			};
			outputs.push(Some(stored));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_get_process_stored_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| {
					Self::try_get_process_stored_and_metadata_batch_sqlite_sync(connection, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_stored_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_command_stored: u64,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			node_output_stored: u64,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_stored: u64,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_stored: u64,
			subtree_count: Option<u64>,
			subtree_stored: u64,
		}
		let statement = indoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_command_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored
				from processes
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut outputs = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
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
			let stored = Output {
				node_command: row.node_command_stored != 0,
				node_output: row.node_output_stored != 0,
				subtree_command: row.subtree_command_stored != 0,
				subtree_output: row.subtree_output_stored != 0,
				subtree: row.subtree_stored != 0,
			};
			let node = tg::process::metadata::Node {
				command: tg::object::metadata::Subtree {
					count: row.node_command_count,
					depth: row.node_command_depth,
					size: row.node_command_size,
				},
				output: tg::object::metadata::Subtree {
					count: row.node_output_count,
					depth: row.node_output_depth,
					size: row.node_output_size,
				},
			};
			let subtree = tg::process::metadata::Subtree {
				command: tg::object::metadata::Subtree {
					count: row.subtree_command_count,
					depth: row.subtree_command_depth,
					size: row.subtree_command_size,
				},
				output: tg::object::metadata::Subtree {
					count: row.subtree_output_count,
					depth: row.subtree_output_depth,
					size: row.subtree_output_size,
				},
				process_count: row.subtree_count,
			};
			let metadata = tg::process::Metadata { node, subtree };
			outputs.push(Some((stored, metadata)));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_touch_process_and_get_stored_and_metadata_sqlite_sync(
						connection, &id, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_process_and_get_stored_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_command_stored: u64,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			node_output_stored: u64,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_stored: u64,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_stored: u64,
			subtree_count: Option<u64>,
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
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
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
		let stored = Output {
			node_command: row.node_command_stored != 0,
			node_output: row.node_output_stored != 0,
			subtree_command: row.subtree_command_stored != 0,
			subtree_output: row.subtree_output_stored != 0,
			subtree: row.subtree_stored != 0,
		};
		let node = tg::process::metadata::Node {
			command: tg::object::metadata::Subtree {
				count: row.node_command_count,
				depth: row.node_command_depth,
				size: row.node_command_size,
			},
			output: tg::object::metadata::Subtree {
				count: row.node_output_count,
				depth: row.node_output_depth,
				size: row.node_output_size,
			},
		};
		let subtree = tg::process::metadata::Subtree {
			command: tg::object::metadata::Subtree {
				count: row.subtree_command_count,
				depth: row.subtree_command_depth,
				size: row.subtree_command_size,
			},
			output: tg::object::metadata::Subtree {
				count: row.subtree_output_count,
				depth: row.subtree_output_depth,
				size: row.subtree_output_size,
			},
			process_count: row.subtree_count,
		};
		let metadata = tg::process::Metadata { node, subtree };
		Ok(Some((stored, metadata)))
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| {
					Self::try_touch_process_and_get_stored_and_metadata_batch_sqlite_sync(
						connection, &ids, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_process_and_get_stored_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_command_stored: u64,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			node_output_stored: u64,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_stored: u64,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_stored: u64,
			subtree_count: Option<u64>,
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
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
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
			let stored = Output {
				node_command: row.node_command_stored != 0,
				node_output: row.node_output_stored != 0,
				subtree_command: row.subtree_command_stored != 0,
				subtree_output: row.subtree_output_stored != 0,
				subtree: row.subtree_stored != 0,
			};
			let node = tg::process::metadata::Node {
				command: tg::object::metadata::Subtree {
					count: row.node_command_count,
					depth: row.node_command_depth,
					size: row.node_command_size,
				},
				output: tg::object::metadata::Subtree {
					count: row.node_output_count,
					depth: row.node_output_depth,
					size: row.node_output_size,
				},
			};
			let subtree = tg::process::metadata::Subtree {
				command: tg::object::metadata::Subtree {
					count: row.subtree_command_count,
					depth: row.subtree_command_depth,
					size: row.subtree_command_size,
				},
				output: tg::object::metadata::Subtree {
					count: row.subtree_output_count,
					depth: row.subtree_output_depth,
					size: row.subtree_output_size,
				},
				process_count: row.subtree_count,
			};
			let metadata = tg::process::Metadata { node, subtree };
			outputs.push(Some((stored, metadata)));
		}
		Ok(outputs)
	}
}
