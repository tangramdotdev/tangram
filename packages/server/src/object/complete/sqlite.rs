use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_object_complete_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| Self::try_get_object_complete_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_complete_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
		}
		let statement = indoc!(
			"
				select complete
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
		Ok(Some(row.complete))
	}

	pub(crate) async fn try_get_object_complete_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
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
				move |connection| Self::try_get_object_complete_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_complete_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
		}
		let statement = indoc!(
			"
				select complete
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
			outputs.push(Some(row.complete));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_get_object_complete_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object complete flag and metadata.
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_get_object_complete_and_metadata_sqlite_sync(connection, &id)
				}
			})
			.await?;

		Ok(output)
	}

	pub(crate) fn try_get_object_complete_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		// Get the object complete flag and metadata.
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				select complete, count, depth, weight
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
		let complete = row.complete;
		let metadata = tg::object::Metadata {
			count: row.count,
			depth: row.depth,
			weight: row.weight,
		};
		Ok(Some((complete, metadata)))
	}

	pub(crate) async fn try_get_object_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
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
					Self::try_get_object_complete_and_metadata_batch_sqlite_sync(connection, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_complete_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				select complete, count, depth, weight
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
			let complete = row.complete;
			let metadata = tg::object::Metadata {
				count: row.count,
				depth: row.depth,
				weight: row.weight,
			};
			outputs.push(Some((complete, metadata)));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_touch_object_and_get_complete_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_touch_object_and_get_complete_and_metadata_sqlite_sync(
						connection, &id, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_object_and_get_complete_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update objects
				set touched_at = max(?1, touched_at)
				where id = ?2
				returning complete, count, depth, weight;
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
		let complete = row.complete;
		let metadata = tg::object::Metadata {
			count: row.count,
			depth: row.depth,
			weight: row.weight,
		};
		Ok(Some((complete, metadata)))
	}

	pub(crate) async fn try_touch_object_and_get_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
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
						Self::try_touch_object_and_get_complete_and_metadata_batch_sqlite_sync(
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

	pub(crate) fn try_touch_object_and_get_complete_and_metadata_batch_sqlite_sync(
		transaction: &sqlite::Transaction,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update objects
				set touched_at = max(?1, touched_at)
				where id = ?2
				returning complete, count, depth, weight;
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
			let complete = row.complete;
			let metadata = tg::object::Metadata {
				count: row.count,
				depth: row.depth,
				weight: row.weight,
			};
			outputs.push(Some((complete, metadata)));
		}
		Ok(outputs)
	}
}
