use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite, tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let outputs = connection
			.with({
				let ids = ids.to_owned();
				move |connection| Self::try_get_process_batch_sqlite_sync(connection, &ids)
			})
			.await?;

		Ok(outputs)
	}

	pub(crate) fn try_get_process_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::get::Output>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			outputs.push(Self::try_get_process_sqlite_sync(connection, id)?);
		}
		Ok(outputs)
	}

	pub(crate) fn try_get_process_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::get::Output>> {
		// Get the process.
		let statement = indoc!(
			"
				select
					actual_checksum,
					cacheable,
					command,
					created_at,
					dequeued_at,
					enqueued_at,
					error,
					exit,
					expected_checksum,
					finished_at,
					host,
					log,
					output,
					retry,
					mounts,
					network,
					started_at,
					status,
					stderr,
					stdin,
					stdout
				from processes
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		// Deserialize the row.
		let actual_checksum = row
			.get::<_, Option<String>>(0)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let cacheable = row
			.get::<_, u64>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let command = row
			.get::<_, String>(2)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.parse()?;
		let created_at = row
			.get::<_, i64>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let dequeued_at = row
			.get::<_, Option<i64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let enqueued_at = row
			.get::<_, Option<i64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let error = row
			.get::<_, Option<String>>(6)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
		let exit = row
			.get::<_, Option<u8>>(7)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let expected_checksum = row
			.get::<_, Option<String>>(8)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let finished_at = row
			.get::<_, Option<i64>>(9)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let host = row
			.get::<_, String>(10)
			.map_err(|source| tg::error!(!source, "expected a string"))?;
		let log = row
			.get::<_, Option<String>>(11)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let output = row
			.get::<_, Option<String>>(12)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
		let retry = row
			.get::<_, u64>(13)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let mounts = row
			.get::<_, Option<String>>(14)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| serde_json::from_str(&s))
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize"))?
			.unwrap_or_default();
		let network = row
			.get::<_, u64>(15)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let started_at = row
			.get::<_, Option<i64>>(16)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let status = row
			.get::<_, String>(17)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.parse()?;
		let stderr = row
			.get::<_, Option<String>>(18)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let stdin = row
			.get::<_, Option<String>>(19)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;
		let stdout = row
			.get::<_, Option<String>>(20)
			.map_err(|source| tg::error!(!source, "expected a string"))?
			.map(|s| s.parse())
			.transpose()?;

		// Get the children.
		let statement = indoc!(
			"
				select child, options
				from process_children
				where process = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut children = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let item = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.parse()?;
			let options = row
				.get::<_, db::value::Json<tg::referent::Options>>(1)
				.map_err(|source| tg::error!(!source, "expected options json"))?
				.0;
			let referent = tg::Referent { item, options };
			children.push(referent);
		}

		let data = tg::process::Data {
			actual_checksum,
			cacheable,
			children: Some(children),
			command,
			created_at,
			dequeued_at,
			enqueued_at,
			error,
			exit,
			expected_checksum,
			finished_at,
			host,
			log,
			output,
			retry,
			mounts,
			network,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
		};

		let output = tg::process::get::Output {
			id: id.clone(),
			data,
		};

		Ok(Some(output))
	}
}
