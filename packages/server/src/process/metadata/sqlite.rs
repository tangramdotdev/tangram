use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection, cache| {
					Self::try_get_process_metadata_sqlite_sync(connection, cache, &id)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		let statement = indoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_error_count,
					node_error_depth,
					node_error_size,
					node_log_count,
					node_log_depth,
					node_log_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
				from processes
				where processes.id = ?1;
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
		let metadata = row.to_metadata();
		Ok(Some(metadata))
	}

	pub(crate) async fn try_get_process_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
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
					Self::try_get_process_metadata_batch_sqlite_sync(connection, cache, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let statement = indoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_error_count,
					node_error_depth,
					node_error_size,
					node_log_count,
					node_log_depth,
					node_log_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
				from processes
				where processes.id = ?1;
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
			let metadata = row.to_metadata();
			outputs.push(Some(metadata));
		}
		Ok(outputs)
	}
}

#[derive(db::sqlite::row::Deserialize)]
struct Row {
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_command_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_command_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_command_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_error_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_error_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_error_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_log_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_log_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_log_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_output_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_output_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	node_output_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_command_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_command_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_command_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_error_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_error_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_error_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_log_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_log_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_log_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_output_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_output_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_output_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	subtree_count: Option<u64>,
}

impl Row {
	fn to_metadata(&self) -> tg::process::Metadata {
		let node = tg::process::metadata::Node {
			command: tg::object::metadata::Subtree {
				count: self.node_command_count,
				depth: self.node_command_depth,
				size: self.node_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: self.node_error_count,
				depth: self.node_error_depth,
				size: self.node_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: self.node_log_count,
				depth: self.node_log_depth,
				size: self.node_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: self.node_output_count,
				depth: self.node_output_depth,
				size: self.node_output_size,
				solvable: None,
				solved: None,
			},
		};
		let subtree = tg::process::metadata::Subtree {
			command: tg::object::metadata::Subtree {
				count: self.subtree_command_count,
				depth: self.subtree_command_depth,
				size: self.subtree_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: self.subtree_error_count,
				depth: self.subtree_error_depth,
				size: self.subtree_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: self.subtree_log_count,
				depth: self.subtree_log_depth,
				size: self.subtree_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: self.subtree_output_count,
				depth: self.subtree_output_depth,
				size: self.subtree_output_size,
				solvable: None,
				solved: None,
			},
			count: self.subtree_count,
		};
		tg::process::Metadata { node, subtree }
	}
}
