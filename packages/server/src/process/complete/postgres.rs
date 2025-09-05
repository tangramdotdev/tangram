use super::Output;
use crate::Server;
use indoc::{formatdoc, indoc};
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn try_get_process_complete_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the metadata.
		#[derive(serde::Deserialize)]
		struct Row {
			pub children_complete: bool,
			pub commands_complete: bool,
			pub outputs_complete: bool,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select children_complete, commands_complete, outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
			.map(|row| Output {
				children: row.children_complete,
				commands: row.commands_complete,
				outputs: row.outputs_complete,
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_complete_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					commands_complete,
					outputs_complete
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
			.map_err(|source| tg::error!(!source, "failed to query the database"))?
			.into_iter()
			.map(|row| {
				row.get::<_, Option<Vec<u8>>>(0)?;
				let children_complete = row.get::<_, bool>(1);
				let commands_complete = row.get::<_, bool>(2);
				let outputs_complete = row.get::<_, bool>(3);
				let output = Output {
					children: children_complete,
					commands: commands_complete,
					outputs: outputs_complete,
				};
				Some(output)
			})
			.collect();
		Ok(outputs)
	}

	pub(crate) async fn try_get_process_complete_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(serde::Deserialize)]
		struct Row {
			pub children_complete: bool,
			pub children_count: Option<u64>,
			pub commands_complete: bool,
			pub commands_count: Option<u64>,
			pub commands_depth: Option<u64>,
			pub commands_weight: Option<u64>,
			pub outputs_complete: bool,
			pub outputs_count: Option<u64>,
			pub outputs_depth: Option<u64>,
			pub outputs_weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					children_complete,
					children_count,
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0);

		// Drop the database connection.
		drop(connection);

		let output = output.map(|output| {
			let children = tg::process::metadata::Children {
				count: output.children_count,
			};
			let commands = tg::object::Metadata {
				count: output.commands_count,
				depth: output.commands_depth,
				weight: output.commands_weight,
			};
			let outputs = tg::object::Metadata {
				count: output.outputs_count,
				depth: output.outputs_depth,
				weight: output.outputs_weight,
			};
			let metadata = tg::process::Metadata {
				children,
				commands,
				outputs,
			};
			let complete = Output {
				children: output.children_complete,
				commands: output.commands_complete,
				outputs: output.outputs_complete,
			};
			(complete, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_get_process_complete_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					children_count,
					commands_count,
					commands_depth,
					commands_weight,
					commands_complete,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
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
			.map_err(|source| tg::error!(!source, "failed to query the database"))?
			.into_iter()
			.map(|row| {
				row.get::<_, Option<Vec<u8>>>(0)?;
				let children_complete = row.get::<_, bool>(1);
				let children_count = row.get::<_, Option<i64>>(2).map(|v| v.to_u64().unwrap());
				let commands_complete = row.get::<_, bool>(3);
				let commands_count = row.get::<_, Option<i64>>(4).map(|v| v.to_u64().unwrap());
				let commands_depth = row.get::<_, Option<i64>>(5).map(|v| v.to_u64().unwrap());
				let commands_weight = row.get::<_, Option<i64>>(6).map(|v| v.to_u64().unwrap());
				let outputs_complete = row.get::<_, bool>(7);
				let outputs_count = row.get::<_, Option<i64>>(8).map(|v| v.to_u64().unwrap());
				let outputs_depth = row.get::<_, Option<i64>>(9).map(|v| v.to_u64().unwrap());
				let outputs_weight = row.get::<_, Option<i64>>(10).map(|v| v.to_u64().unwrap());
				let complete = Output {
					children: children_complete,
					commands: commands_complete,
					outputs: outputs_complete,
				};
				let children = tg::process::metadata::Children {
					count: children_count,
				};
				let commands = tg::object::Metadata {
					count: commands_count,
					depth: commands_depth,
					weight: commands_weight,
				};
				let outputs = tg::object::Metadata {
					count: outputs_count,
					depth: outputs_depth,
					weight: outputs_weight,
				};
				let metadata = tg::process::Metadata {
					children,
					commands,
					outputs,
				};
				Some((complete, metadata))
			})
			.collect();
		Ok(outputs)
	}
}
