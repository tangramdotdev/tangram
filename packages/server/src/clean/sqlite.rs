use {
	super::{Count, InnerOutput, Server},
	bytes::Bytes,
	indoc::{formatdoc, indoc},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn clean_count_items_sqlite(
		&self,
		database: &db::sqlite::Database,
		max_touched_at: i64,
	) -> tg::Result<Count> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;
		let statement = indoc!(
			"
				select
					(select count(*) from cache_entries where reference_count = 0 and touched_at < ?1) as cache_entries,
					(select count(*) from objects where reference_count = 0 and touched_at < ?1) as objects,
					(select count(*) from processes where reference_count = 0 and touched_at < ?1) as processes;
				;
			"
		);
		let params = db::params![max_touched_at];
		let count = connection
			.query_one_into::<db::row::Serde<Count>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.0;
		Ok(count)
	}

	pub(super) async fn cleaner_task_inner_sqlite(
		&self,
		database: &db::sqlite::Database,
		max_touched_at: i64,
		mut n: usize,
	) -> tg::Result<InnerOutput> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from cache_entries
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let cache_entries_ = connection
			.query_all_value_into::<Bytes>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|bytes| tg::artifact::Id::from_slice(&bytes))
			.collect::<tg::Result<Vec<_>>>()?;

		let mut cache_entries = Vec::new();
		for id in cache_entries_ {
			let statement = formatdoc!(
				"
					update cache_entries
						set reference_count = (
							select count(*) from objects where cache_entry = ?1
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id.to_bytes()];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						delete from cache_entries
						where id = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				cache_entries.push(id);
			}
		}
		n -= cache_entries.len();

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from objects
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let objects_ = connection
			.query_all_value_into::<Bytes>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|bytes| tg::object::Id::from_slice(&bytes))
			.collect::<tg::Result<Vec<_>>>()?;

		let mut objects = Vec::new();
		for id in objects_ {
			let statement = formatdoc!(
				"
					update objects
						set reference_count = (
							(select count(*) from object_children where child = ?1) +
							(select count(*) from process_objects where object = ?1) +
							(select count(*) from tags where item = ?1)
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id.to_bytes()];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						update objects
						set reference_count = reference_count - 1
						where id in (
							select child from object_children where object = ?1
						);
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						update cache_entries
						set reference_count = reference_count - 1
						where id in (
							select cache_entry from objects where id = ?1
						);
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from objects
						where id = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from object_children
						where object = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				objects.push(id);
			}
		}
		n -= objects.len();

		let p = connection.p();
		let statement = formatdoc!(
			"
				select id from processes
				where reference_count = 0 and touched_at <= {p}1
				limit {p}2;
			"
		);
		let params = db::params![max_touched_at, n];
		let processes_ = connection
			.query_all_value_into::<Bytes>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|bytes| tg::process::Id::from_slice(&bytes))
			.collect::<tg::Result<Vec<_>>>()?;

		let mut processes = Vec::new();
		for id in processes_ {
			let statement = formatdoc!(
				"
					update processes
						set reference_count = (
							(select count(*) from process_children where child = ?1) +
							(select count(*) from tags where item = ?1)
						),
						reference_count_transaction_id = (
							select id from transaction_id
						)
					where id = ?1
					returning reference_count;
				"
			);
			let params = db::params![id.to_bytes()];
			let reference_count = connection
				.query_one_value_into::<u64>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if reference_count == 0 {
				let statement = formatdoc!(
					"
						update processes
							set reference_count = reference_count - 1
						where id in (
							select child from process_children
							where process = ?1
						);
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						update objects
							set reference_count = reference_count - 1
						where id in (
							select object from process_objects
							where process = ?1
						);
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from process_children
						where process = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from process_objects
						where process = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let statement = formatdoc!(
					"
						delete from processes
						where id = ?1;
					"
				);
				let params = db::params![id.to_bytes()];
				connection
					.execute(statement.into(), params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				processes.push(id);
			}
		}

		// Drop the connection.
		drop(connection);

		let output = InnerOutput {
			cache_entries,
			objects,
			processes,
		};

		Ok(output)
	}
}
