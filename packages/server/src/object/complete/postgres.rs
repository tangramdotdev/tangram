use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	std::collections::HashMap,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
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
				row.get::<_, Option<Vec<u8>>>(0)?;
				let complete = row.get::<_, bool>(1);
				Some(complete)
			})
			.collect();
		Ok(outputs)
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

		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
				returning complete, count, depth, weight;
			",
		);
		#[derive(serde::Deserialize)]
		struct Row {
			complete: bool,
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
		let params = db::params![touched_at, id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0);

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
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				from unnest($2::bytea[]) as ids (id)
				where objects.id = ids.id
				returning objects.id, complete, count, depth, weight;
			",
		);
		let output = connection
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
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				let id = row.get::<_, Vec<u8>>(0);
				let id = tg::object::Id::from_slice(&id).unwrap();
				let complete = row.get::<_, bool>(1);
				let count = row.get::<_, Option<i64>>(2).map(|v| v.to_u64().unwrap());
				let depth = row.get::<_, Option<i64>>(3).map(|v| v.to_u64().unwrap());
				let weight = row.get::<_, Option<i64>>(4).map(|v| v.to_u64().unwrap());
				let metadata = tg::object::Metadata {
					count,
					depth,
					weight,
				};
				(id, (complete, metadata))
			})
			.collect::<HashMap<_, _, fnv::FnvBuildHasher>>();
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}
}
