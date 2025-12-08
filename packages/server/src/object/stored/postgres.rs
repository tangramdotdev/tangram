use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	std::{collections::HashMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_util::iter::TryExt as _,
};

impl Server {
	pub(crate) async fn try_get_object_stored_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<super::Output>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object subtree stored flag.
		let statement = indoc!(
			"
				select subtree_stored
				from objects
				where id = $1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output: Option<bool> = connection
			.query_optional_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		Ok(output.map(|subtree| super::Output { subtree }))
	}

	pub(crate) async fn try_get_object_stored_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<Vec<u8>>>")]
			id: Option<tg::object::Id>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					objects.id,
					subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join objects on objects.id = ids.id;
			",
		);
		let ids = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let outputs = connection
			.inner()
			.query(statement, &[&ids])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.and_then(|row| {
				Ok(row.id.is_some().then_some(super::Output {
					subtree: row.subtree_stored,
				}))
			})
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub(crate) async fn try_get_object_stored_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object subtree stored flag and metadata.
		#[derive(db::row::Deserialize)]
		struct Row {
			node_size: Option<u64>,
			subtree_count: Option<u64>,
			subtree_depth: Option<u64>,
			subtree_size: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select node_size, subtree_count, subtree_depth, subtree_size, subtree_stored
				from objects
				where id = $1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		let output = output.map(|output| {
			let stored = super::Output {
				subtree: output.subtree_stored,
			};
			let metadata = tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: output.node_size,
				},
				subtree: tg::object::metadata::Subtree {
					count: output.subtree_count,
					depth: output.subtree_depth,
					size: output.subtree_size,
				},
			};
			(stored, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_get_object_stored_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(try_from = "Vec<u8>")]
			id: tg::object::Id,
			node_size: Option<i64>,
			subtree_count: Option<i64>,
			subtree_depth: Option<i64>,
			subtree_size: Option<i64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select objects.id, node_size, subtree_count, subtree_depth, subtree_size, subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join objects on objects.id = ids.id;
			",
		);
		let output = connection
			.inner()
			.query(
				statement,
				&[&ids
					.iter()
					.map(|id| id.to_bytes().to_vec())
					.collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let stored = super::Output {
					subtree: row.subtree_stored,
				};
				let metadata = tg::object::Metadata {
					node: tg::object::metadata::Node {
						size: row.node_size.map(|v| v.to_u64().unwrap()),
					},
					subtree: tg::object::metadata::Subtree {
						count: row.subtree_count.map(|v| v.to_u64().unwrap()),
						depth: row.subtree_depth.map(|v| v.to_u64().unwrap()),
						size: row.subtree_size.map(|v| v.to_u64().unwrap()),
					},
				};
				Ok((row.id, (stored, metadata)))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			node_size: Option<u64>,
			subtree_count: Option<u64>,
			subtree_depth: Option<u64>,
			subtree_size: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
				returning node_size, subtree_count, subtree_depth, subtree_size, subtree_stored;
			",
		);
		let params = db::params![touched_at, id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		let output = output.map(|output| {
			let stored = super::Output {
				subtree: output.subtree_stored,
			};
			let metadata = tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: output.node_size,
				},
				subtree: tg::object::metadata::Subtree {
					count: output.subtree_count,
					depth: output.subtree_depth,
					size: output.subtree_size,
				},
			};
			(stored, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
		let options = tangram_futures::retry::Options::default();
		tangram_futures::retry(&options, || {
			self.try_touch_object_and_get_stored_and_metadata_batch_postgres_attempt(
				database, ids, touched_at,
			)
		})
		.await
	}

	async fn try_touch_object_and_get_stored_and_metadata_batch_postgres_attempt(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<ControlFlow<Vec<Option<(super::Output, tg::object::Metadata)>>, tg::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(vec![]));
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(try_from = "Vec<u8>")]
			id: tg::object::Id,
			node_size: Option<i64>,
			subtree_count: Option<i64>,
			subtree_depth: Option<i64>,
			subtree_size: Option<i64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				with locked as (
					select objects.id
					from objects
					join unnest($2::bytea[]) as ids (id) on objects.id = ids.id
					order by objects.id
					for update
				)
				update objects
				set touched_at = greatest($1::int8, touched_at)
				from locked
				where objects.id = locked.id
				returning objects.id, node_size, subtree_count, subtree_depth, subtree_size, subtree_stored;
			",
		);
		let result = connection
			.inner()
			.query(
				statement,
				&[
					&touched_at,
					&ids.iter()
						.map(|id| id.to_bytes().to_vec())
						.collect::<Vec<_>>(),
				],
			)
			.await;
		let output = match result {
			Ok(output) => output,
			Err(error) if db::postgres::util::error_is_retryable(&error) => {
				let error = tg::error!(!error, "failed to execute the statement");
				return Ok(ControlFlow::Continue(error));
			},
			Err(error) => {
				let error = tg::error!(!error, "failed to execute the statement");
				return Err(error);
			},
		};
		let output = output
			.into_iter()
			.map(|row| {
				let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let stored = super::Output {
					subtree: row.subtree_stored,
				};
				let metadata = tg::object::Metadata {
					node: tg::object::metadata::Node {
						size: row.node_size.map(|v| v.to_u64().unwrap()),
					},
					subtree: tg::object::metadata::Subtree {
						count: row.subtree_count.map(|v| v.to_u64().unwrap()),
						depth: row.subtree_depth.map(|v| v.to_u64().unwrap()),
						size: row.subtree_size.map(|v| v.to_u64().unwrap()),
					},
				};
				Ok((row.id, (stored, metadata)))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
