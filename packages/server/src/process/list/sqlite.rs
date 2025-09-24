use {
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite, tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn list_processes_sqlite(
		&self,
		database: &db::sqlite::Database,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let outputs = connection
			.with(move |connection| Self::list_processes_sqlite_sync(connection))
			.await?;

		Ok(outputs)
	}

	pub(crate) fn list_processes_sqlite_sync(
		connection: &sqlite::Connection,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		let statement = indoc!(
			"
				select
					id,
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
				where status != 'finished';
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let mut outputs = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let id = row
				.get::<_, String>(0)
				.map_err(|source| tg::error!(!source, "expected a string"))?;
			let actual_checksum = row
				.get::<_, Option<String>>(1)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;
			let cacheable = row
				.get::<_, u64>(2)
				.map_err(|source| tg::error!(!source, "expected an integer"))?
				!= 0;
			let command = row
				.get::<_, String>(3)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.parse()?;
			let created_at = row
				.get::<_, i64>(4)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let dequeued_at = row
				.get::<_, Option<i64>>(5)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let enqueued_at = row
				.get::<_, Option<i64>>(6)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let error = row
				.get::<_, Option<String>>(7)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| serde_json::from_str(&s))
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
			let exit = row
				.get::<_, Option<u8>>(8)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let expected_checksum = row
				.get::<_, Option<String>>(9)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;
			let finished_at = row
				.get::<_, Option<i64>>(10)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let host = row
				.get::<_, String>(11)
				.map_err(|source| tg::error!(!source, "expected a string"))?;
			let log = row
				.get::<_, Option<String>>(12)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;
			let output = row
				.get::<_, Option<String>>(13)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| serde_json::from_str(&s))
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
			let retry = row
				.get::<_, u64>(14)
				.map_err(|source| tg::error!(!source, "expected an integer"))?
				!= 0;
			let mounts = row
				.get::<_, Option<String>>(15)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| serde_json::from_str(&s))
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to deserialize"))?
				.unwrap_or_default();
			let network = row
				.get::<_, u64>(16)
				.map_err(|source| tg::error!(!source, "expected an integer"))?
				!= 0;
			let started_at = row
				.get::<_, Option<i64>>(17)
				.map_err(|source| tg::error!(!source, "expected an integer"))?;
			let status = row
				.get::<_, String>(18)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.parse()?;
			let stderr = row
				.get::<_, Option<String>>(19)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;
			let stdin = row
				.get::<_, Option<String>>(20)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;
			let stdout = row
				.get::<_, Option<String>>(21)
				.map_err(|source| tg::error!(!source, "expected a string"))?
				.map(|s| s.parse())
				.transpose()?;

			// Get the children for this process
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
				.query([&id])
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

			let output = tg::process::get::Output { data };
			outputs.push(output);
		}

		Ok(outputs)
	}
}
