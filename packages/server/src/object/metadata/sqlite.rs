use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_object_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection, cache| {
					Self::try_get_object_metadata_sqlite_sync(connection, cache, &id)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
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
					subtree_solved
				from objects
				where id = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_bytes().to_vec()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
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
		Ok(Some(metadata))
	}

	pub(crate) async fn try_get_object_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
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
				move |connection, cache| {
					Self::try_get_object_metadata_batch_sqlite_sync(connection, cache, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
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
					subtree_solved
				from objects
				where id = ?1;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
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
			outputs.push(Some(metadata));
		}
		Ok(outputs)
	}
}
