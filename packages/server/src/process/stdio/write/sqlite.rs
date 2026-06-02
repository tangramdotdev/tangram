use {
	super::WriteOutput,
	crate::{Session, process::stdio::MAX_UNREAD_PROCESS_STDIO_BYTES},
	bytes::Bytes,
	rusqlite as sqlite,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn try_write_process_stdio_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<WriteOutput> {
		let id = id.clone();
		let bytes = bytes.clone();
		process_store
			.run(move |transaction, _cache| {
				Self::try_write_process_stdio_sqlite_sync(transaction, &id, stream, &bytes)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to write process stdio"))
	}

	fn try_write_process_stdio_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: &Bytes,
	) -> tg::Result<ControlFlow<WriteOutput, db::sqlite::Error>> {
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
		let result = transaction
			.prepare(&statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let mut rows = {
			let result = statement
				.query(sqlite::params![id.to_string()])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement")
		};
		let result = rows.next().map_err(db::sqlite::Error::from);
		let Some(row) = crate::database::retry!(result, "failed to execute the statement") else {
			return Ok(ControlFlow::Break(WriteOutput::Closed));
		};
		let result = row
			.get::<_, Option<bool>>(0)
			.map_err(db::sqlite::Error::from);
		let open = crate::database::retry!(result, "failed to get the open state");
		if open != Some(true) {
			return Ok(ControlFlow::Break(WriteOutput::Closed));
		}
		drop(rows);
		drop(statement);

		let statement = "
			select coalesce(sum(length(bytes)), 0)
			from process_stdio
			where process = ?1 and stream = ?2;
		";
		let result = transaction
			.query_row(
				statement,
				sqlite::params![id.to_string(), stream.to_string()],
				|row| row.get::<_, i64>(0),
			)
			.map_err(db::sqlite::Error::from);
		let written_len = crate::database::retry!(result, "failed to execute the statement");
		let written_len = u64::try_from(written_len).unwrap();
		let bytes_len = u64::try_from(bytes.len()).unwrap();
		if written_len != 0
			&& written_len.saturating_add(bytes_len) > MAX_UNREAD_PROCESS_STDIO_BYTES
		{
			return Ok(ControlFlow::Break(WriteOutput::Full));
		}

		let statement = "
			insert into process_stdio (process, stream, bytes)
			values (?1, ?2, ?3);
		";
		let result = transaction
			.execute(
				statement,
				sqlite::params![id.to_string(), stream.to_string(), bytes.as_ref()],
			)
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(WriteOutput::Written))
	}

	pub(crate) async fn try_close_process_stdio_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		let id = id.clone();
		process_store
			.run(move |transaction, _cache| {
				Self::try_close_process_stdio_sqlite_sync(transaction, &id, stream)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to close process stdio"))
	}

	fn try_close_process_stdio_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<ControlFlow<(), db::sqlite::Error>> {
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
		let result = transaction
			.execute(&statement, sqlite::params![id.to_string()])
			.map_err(db::sqlite::Error::from);
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}
}
