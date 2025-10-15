use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn list_processes_postgres(
		&self,
		database: &db::postgres::Database,
	) -> tg::Result<Vec<tg::process::get::Output>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let statement = indoc!(
			"
				select
					processes.id,
					actual_checksum,
					cacheable,
					(select coalesce(array_agg(child), '{}') from process_children where process = processes.id) as children,
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

		let outputs = connection
			.inner()
			.query(statement, &[])
			.await
			.map_err(|source| tg::error!(!source, "failed to query the database"))?
			.into_iter()
			.map(|row| {
				let id = row.get::<_, String>(0).parse()?;
				let actual_checksum = row
					.get::<_, Option<String>>(1)
					.map(|s| s.parse())
					.transpose()?;
				let cacheable = row.get::<_, i64>(2) != 0;
				let children = row
					.get::<_, Vec<String>>(3)
					.into_iter()
					.map(|id| id.parse())
					.collect::<tg::Result<_>>()?;
				let command = row.get::<_, String>(4).parse()?;
				let created_at = row.get::<_, i64>(5);
				let dequeued_at = row.get::<_, Option<i64>>(6);
				let enqueued_at = row.get::<_, Option<i64>>(7);
				let error = row
					.get::<_, Option<String>>(8)
					.map(|s| serde_json::from_str(&s))
					.transpose()
					.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
				let exit = row.get::<_, Option<i64>>(9).map(|v| v.to_u8().unwrap());
				let expected_checksum = row
					.get::<_, Option<String>>(10)
					.map(|s| s.parse())
					.transpose()?;
				let finished_at = row.get::<_, Option<i64>>(11);
				let host = row.get::<_, String>(12);
				let log = row
					.get::<_, Option<String>>(13)
					.map(|s| s.parse())
					.transpose()
					.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
				let output = row
					.get::<_, Option<String>>(14)
					.map(|s| serde_json::from_str(&s))
					.transpose()
					.map_err(|source| tg::error!(!source, "failed to deserialize"))?;
				let retry = row.get::<_, i64>(15) != 0;
				let mounts = row
					.get::<_, Option<String>>(16)
					.map(|s| serde_json::from_str(&s))
					.transpose()
					.map_err(|source| tg::error!(!source, "failed to deserialize"))?
					.unwrap_or_default();
				let network = row.get::<_, i64>(17) != 0;
				let started_at = row.get::<_, Option<i64>>(18);
				let status = row.get::<_, String>(19).parse()?;
				let stderr = row
					.get::<_, Option<String>>(20)
					.map(|s| s.parse())
					.transpose()?;
				let stdin = row
					.get::<_, Option<String>>(21)
					.map(|s| s.parse())
					.transpose()?;
				let stdout = row
					.get::<_, Option<String>>(22)
					.map(|s| s.parse())
					.transpose()?;
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
				let output = tg::process::get::Output { id, data };
				Ok::<_, tg::Error>(output)
			})
			.collect::<tg::Result<_>>()?;

		Ok(outputs)
	}
}
