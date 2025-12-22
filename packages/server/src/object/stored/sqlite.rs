use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_object_stored_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<super::Output>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| Self::try_get_object_stored_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_stored_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<super::Output>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select subtree_stored
				from objects
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
		let stored = super::Output {
			subtree: row.subtree_stored,
		};
		Ok(Some(stored))
	}

	pub(crate) async fn try_get_object_stored_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Output>>> {
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
				move |connection| Self::try_get_object_stored_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_stored_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select subtree_stored
				from objects
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
			let stored = super::Output {
				subtree: row.subtree_stored,
			};
			outputs.push(Some(stored));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_get_object_stored_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_get_object_stored_and_metadata_sqlite_sync(connection, &id)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_stored_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
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
				select
					node_size,
					node_solvable,
					node_solved,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved,
					subtree_stored
				from objects
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
		let stored = super::Output {
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

	pub(crate) async fn try_get_object_stored_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
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
					Self::try_get_object_stored_and_metadata_batch_sqlite_sync(connection, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_stored_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
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
				select
					node_size,
					node_solvable,
					node_solved,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved,
					subtree_stored
				from objects
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
			let stored = super::Output {
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

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_touch_object_and_get_stored_and_metadata_sqlite_sync(
						connection, &id, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_object_and_get_stored_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
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
		let stored = super::Output {
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

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
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
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
					let output =
						Self::try_touch_object_and_get_stored_and_metadata_batch_sqlite_sync(
							&transaction,
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

	pub(crate) fn try_touch_object_and_get_stored_and_metadata_batch_sqlite_sync(
		transaction: &sqlite::Transaction,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
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
		let mut statement = transaction
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
			let stored = super::Output {
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
}
