use {
	super::Index,
	crate::{ObjectStored, ProcessStored},
	indoc::indoc,
	itertools::Itertools as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_util::iter::TryExt as _,
};

impl Index {
	pub async fn try_get_object_stored(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<ObjectStored>> {
		// Get an index connection.
		let connection = self
			.database
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

		Ok(output.map(|subtree| ObjectStored { subtree }))
	}

	pub async fn try_get_object_stored_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<ObjectStored>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = self
			.database
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
				Ok(row.id.is_some().then_some(ObjectStored {
					subtree: row.subtree_stored,
				}))
			})
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub async fn try_get_object_stored_and_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(ObjectStored, tg::object::Metadata)>> {
		// Get an index connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object subtree stored flag and metadata.
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

	pub async fn try_get_object_stored_and_metadata_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(ObjectStored, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = self
			.database
			.connection()
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
				select objects.id, node_size, node_solvable, node_solved, subtree_count, subtree_depth, subtree_size, subtree_solvable, subtree_solved, subtree_stored
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
		Ok(output)
	}

	pub async fn try_get_process_stored(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<ProcessStored>> {
		// Get an index connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the metadata.
		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_stored: bool,
			node_error_stored: bool,
			node_log_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_error_stored: bool,
			subtree_log_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					node_command_stored,
					node_error_stored,
					node_log_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_error_stored,
					subtree_log_stored,
					subtree_output_stored,
					subtree_stored
				from processes
				where id = $1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| ProcessStored {
				node_command: row.node_command_stored,
				node_error: row.node_error_stored,
				node_log: row.node_log_stored,
				node_output: row.node_output_stored,
				subtree: row.subtree_stored,
				subtree_command: row.subtree_command_stored,
				subtree_error: row.subtree_error_stored,
				subtree_log: row.subtree_log_stored,
				subtree_output: row.subtree_output_stored,
			});

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub async fn try_get_process_stored_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<ProcessStored>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<Vec<u8>>>")]
			id: Option<tg::process::Id>,
			node_command_stored: bool,
			node_error_stored: bool,
			node_log_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_error_stored: bool,
			subtree_log_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					node_command_stored,
					node_error_stored,
					node_log_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_error_stored,
					subtree_log_stored,
					subtree_output_stored,
					subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join processes on processes.id = ids.id;
			",
		);
		let outputs = connection
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
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| {
				row.id?;
				let output = ProcessStored {
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
				Some(output)
			})
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub async fn try_get_process_stored_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(ProcessStored, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = self
			.database
			.connection()
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
				select
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
					subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join processes on processes.id = ids.id;
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
		Ok(output)
	}
}
