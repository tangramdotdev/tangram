use {
	crate::Server,
	indoc::{formatdoc, indoc},
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_object_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object metadata.
		#[derive(db::row::Deserialize)]
		struct Row {
			node_size: u64,
			node_solvable: bool,
			node_solved: bool,
			subtree_count: Option<u64>,
			subtree_depth: Option<u64>,
			subtree_size: Option<u64>,
			subtree_solvable: Option<bool>,
			subtree_solved: Option<bool>,
		}
		let p = connection.p();
		let statement = formatdoc!(
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
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| tg::object::Metadata {
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
			});

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_object_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object metadata.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(try_from = "Vec<u8>")]
			id: tg::object::Id,
			#[tangram_database(try_from = "i64")]
			node_size: u64,
			node_solvable: bool,
			node_solved: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_size: Option<u64>,
			subtree_solvable: Option<bool>,
			subtree_solved: Option<bool>,
		}
		let statement = indoc!(
			"
				select
					objects.id,
					node_size,
					node_solvable,
					node_solved,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved
				from unnest($1::bytea[]) as ids (id)
				left join objects on objects.id = ids.id;
			",
		);
		let ids_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let output = connection
			.inner()
			.query(statement, &[&ids_bytes])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
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
				Ok((row.id, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;

		// Drop the connection.
		drop(connection);

		// Return the results in the same order as the input ids.
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}
}
