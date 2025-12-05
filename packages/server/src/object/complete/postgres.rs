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
	pub(crate) async fn try_get_object_complete_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object metadata.
		let statement = indoc!(
			"
				select complete
				from objects
				where id = $1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_object_complete_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
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
			complete: bool,
		}
		let statement = indoc!(
			"
				select
					objects.id,
					complete
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
			.and_then(|row| Ok(row.id.is_some().then_some(row.complete)))
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub(crate) async fn try_get_object_complete_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object complete flag and metadata.
		#[derive(db::row::Deserialize)]
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
			let metadata = tg::object::Metadata {
				count: output.count,
				depth: output.depth,
				weight: output.weight,
			};
			(output.complete, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_get_object_complete_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
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
			complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				select objects.id, complete, count, depth, weight
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
				let metadata = tg::object::Metadata {
					count: row.count.map(|v| v.to_u64().unwrap()),
					depth: row.depth.map(|v| v.to_u64().unwrap()),
					weight: row.weight.map(|v| v.to_u64().unwrap()),
				};
				Ok((row.id, (row.complete, metadata)))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}

	pub(crate) async fn try_touch_object_and_get_complete_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
				returning complete, count, depth, weight;
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
			let metadata = tg::object::Metadata {
				count: output.count,
				depth: output.depth,
				weight: output.weight,
			};
			(output.complete, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_touch_object_and_get_complete_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		let options = tangram_futures::retry::Options::default();
		tangram_futures::retry(&options, || {
			self.try_touch_object_and_get_complete_and_metadata_batch_postgres_attempt(
				database, ids, touched_at,
			)
		})
		.await
	}

	async fn try_touch_object_and_get_complete_and_metadata_batch_postgres_attempt(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<ControlFlow<Vec<Option<(bool, tg::object::Metadata)>>, tg::Error>> {
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
			complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			weight: Option<u64>,
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
				returning objects.id, complete, count, depth, weight;
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
				let metadata = tg::object::Metadata {
					count: row.count.map(|v| v.to_u64().unwrap()),
					depth: row.depth.map(|v| v.to_u64().unwrap()),
					weight: row.weight.map(|v| v.to_u64().unwrap()),
				};
				Ok((row.id, (row.complete, metadata)))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
