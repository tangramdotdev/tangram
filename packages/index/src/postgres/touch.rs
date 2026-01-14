use {
	super::Index,
	crate::{ObjectStored, ProcessStored},
	indoc::indoc,
	itertools::Itertools as _,
	std::{collections::HashMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures as futures_ext, time,
};

impl Index {
	pub async fn touch_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				where id = $2;
			"
		);
		let params = db::params![touched_at, id.to_bytes()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
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
		let statement = indoc!(
			"
				update processes
				set touched_at = greatest($1::int8, touched_at)
				where id = $2;
			"
		);
		let params = db::params![touched_at, id.to_bytes()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		drop(connection);
		if n == 0 {
			return Err(tg::error!("failed to find the process"));
		}
		Ok(())
	}

	pub async fn try_touch_object_and_get_stored_and_metadata(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

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
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				update objects
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
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
			let stored = ObjectStored {
				subtree: output.subtree_stored,
			};
			let metadata = tg::object::Metadata {
				node: tg::object::metadata::Node {
					size: output.node_size,
					solvable: output.node_solvable,
					solved: output.node_solved,
				},
				subtree: tg::object::metadata::Subtree {
					count: output.subtree_count,
					depth: output.subtree_depth,
					size: output.subtree_size,
					solvable: output.subtree_solvable,
					solved: output.subtree_solved,
				},
			};
			(stored, metadata)
		});

		Ok(output)
	}

	pub async fn try_touch_object_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		let options = futures_ext::retry::Options::default();
		futures_ext::retry(&options, || {
			self.try_touch_object_and_get_stored_and_metadata_batch_attempt(ids, touched_at)
		})
		.await
	}

	async fn try_touch_object_and_get_stored_and_metadata_batch_attempt(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<ControlFlow<Vec<Option<(ObjectStored, tg::object::Metadata)>>, tg::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(vec![]));
		}
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
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
				returning
					objects.id,
					node_size,
					node_solvable,
					node_solved,
					subtree_count,
					subtree_depth,
					subtree_size,
					subtree_solvable,
					subtree_solved,
					subtree_stored;
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
				Ok((row.id, (stored, metadata)))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}

	pub async fn try_touch_process_and_get_stored_and_metadata(
		&self,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(ProcessStored, tg::process::Metadata)>> {
		// Get a database connection.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_command_stored: bool,
			node_error_count: Option<u64>,
			node_error_depth: Option<u64>,
			node_error_size: Option<u64>,
			node_error_stored: bool,
			node_log_count: Option<u64>,
			node_log_depth: Option<u64>,
			node_log_size: Option<u64>,
			node_log_stored: bool,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			node_output_stored: bool,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			subtree_error_count: Option<u64>,
			subtree_error_depth: Option<u64>,
			subtree_error_size: Option<u64>,
			subtree_error_stored: bool,
			subtree_log_count: Option<u64>,
			subtree_log_depth: Option<u64>,
			subtree_log_size: Option<u64>,
			subtree_log_stored: bool,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			subtree_count: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				update processes
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
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
			",
		);
		let params = db::params![touched_at, id.to_bytes()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		let output = row.map(|row| {
			let stored = ProcessStored {
				node_command: row.node_command_stored,
				node_error: row.node_error_stored,
				node_log: row.node_log_stored,
				node_output: row.node_output_stored,
				subtree: row.subtree_stored,
				subtree_command: row.subtree_command_stored,
				subtree_error: row.subtree_error_stored,
				subtree_log: row.subtree_log_stored,
				subtree_output: row.subtree_output_stored,
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
			(stored, metadata)
		});

		Ok(output)
	}

	pub async fn try_touch_process_and_get_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		let options = futures_ext::retry::Options::default();
		futures_ext::retry(&options, || {
			self.try_touch_process_and_get_stored_and_metadata_batch_attempt(ids, touched_at)
		})
		.await
	}

	async fn try_touch_process_and_get_stored_and_metadata_batch_attempt(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<ControlFlow<Vec<Option<(ProcessStored, tg::process::Metadata)>>, tg::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(vec![]));
		}
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<Vec<u8>>")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_size: Option<u64>,
			node_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_size: Option<u64>,
			node_error_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_size: Option<u64>,
			node_log_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_size: Option<u64>,
			node_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_size: Option<u64>,
			subtree_error_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_size: Option<u64>,
			subtree_log_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				with locked as (
					select processes.id
					from processes
					join unnest($2::bytea[]) as ids (id) on processes.id = ids.id
					order by processes.id
					for update
				)
				update processes
				set touched_at = greatest($1::int8, touched_at)
				from locked
				where processes.id = locked.id
				returning
					processes.id,
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
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| {
				let stored = ProcessStored {
					node_command: row.node_command_stored,
					node_error: row.node_error_stored,
					node_log: row.node_log_stored,
					node_output: row.node_output_stored,
					subtree: row.subtree_stored,
					subtree_command: row.subtree_command_stored,
					subtree_error: row.subtree_error_stored,
					subtree_log: row.subtree_log_stored,
					subtree_output: row.subtree_output_stored,
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
				(row.id, (stored, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
