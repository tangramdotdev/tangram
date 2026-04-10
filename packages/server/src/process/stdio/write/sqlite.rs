use {
	crate::{Server, process::stdio::MAX_UNREAD_PROCESS_STDIO_BYTES},
	bytes::Bytes,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

use super::WriteOutput;

impl Server {
	pub(crate) async fn try_write_process_stdio_sqlite(
		&self,
		sandbox_store: &db::sqlite::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<WriteOutput> {
		let id = id.clone();
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_write_process_stdio_sqlite_sync(connection, &id, stream, &bytes)
			})
			.await
	}

	fn try_write_process_stdio_sqlite_sync(
		connection: &mut sqlite::Connection,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: &Bytes,
	) -> tg::Result<WriteOutput> {
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		let statement = format!(
			"
				select {column}
				from processes
				where id = ?1;
			"
		);
		let mut statement = transaction
			.prepare(&statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(WriteOutput::Closed);
		};
		let open = row
			.get::<_, Option<bool>>(0)
			.map_err(|source| tg::error!(!source, "failed to get the open state"))?;
		if open != Some(true) {
			return Ok(WriteOutput::Closed);
		}
		drop(rows);
		drop(statement);

		let statement = "
			select coalesce(sum(length(bytes)), 0)
			from process_stdio
			where process = ?1 and stream = ?2;
		";
		let written_len = transaction
			.query_row(
				statement,
				sqlite::params![id.to_string(), stream.to_string()],
				|row| row.get::<_, i64>(0),
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let written_len = u64::try_from(written_len).unwrap();
		let bytes_len = u64::try_from(bytes.len()).unwrap();
		if written_len != 0
			&& written_len.saturating_add(bytes_len) > MAX_UNREAD_PROCESS_STDIO_BYTES
		{
			return Ok(WriteOutput::Full);
		}

		let statement = "
			insert into process_stdio (process, stream, bytes)
			values (?1, ?2, ?3);
		";
		transaction
			.execute(
				statement,
				sqlite::params![id.to_string(), stream.to_string(), bytes.as_ref()],
			)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
		Ok(WriteOutput::Written)
	}

	pub(crate) async fn try_close_process_stdio_sqlite(
		&self,
		sandbox_store: &db::sqlite::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		let id = id.clone();
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_close_process_stdio_sqlite_sync(connection, &id, stream)
			})
			.await
	}

	fn try_close_process_stdio_sqlite_sync(
		connection: &mut sqlite::Connection,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		let statement = format!(
			"
				update processes
				set {column} = 0
				where id = ?1;
			"
		);
		connection
			.execute(&statement, sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
