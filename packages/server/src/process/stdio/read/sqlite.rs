use {
	crate::Server,
	bytes::Bytes,
	rusqlite as sqlite,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, Database as _},
};

impl Server {
	pub(crate) async fn try_read_process_stdio_pipe_event_sqlite(
		&self,
		sandbox_store: &db::sqlite::Database,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let id = id.clone();
		let streams = streams.clone();
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		connection
			.with(move |connection, _cache| {
				Self::try_read_process_stdio_pipe_event_sqlite_sync(connection, &id, &streams)
			})
			.await
	}

	fn try_read_process_stdio_pipe_event_sqlite_sync(
		connection: &mut sqlite::Connection,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let transaction = connection
			.transaction()
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let placeholders = (0..streams.len())
			.map(|index| format!("?{}", index + 2))
			.collect::<Vec<_>>()
			.join(", ");
		let statement = format!(
			"
				select position, stream, bytes
				from process_stdio
				where process = ?1 and stream in ({placeholders})
				order by position
				limit 1;
			"
		);
		let mut statement = transaction
			.prepare(&statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut params = Vec::with_capacity(streams.len() + 1);
		params.push(id.to_string());
		params.extend(streams.iter().map(ToString::to_string));
		let mut rows = statement
			.query(sqlite::params_from_iter(params.iter()))
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let position = row
				.get::<_, i64>(0)
				.map_err(|source| tg::error!(!source, "failed to get the position"))?;
			let stream = row
				.get::<_, String>(1)
				.map_err(|source| tg::error!(!source, "failed to get the stream"))?
				.parse()
				.map_err(|source| tg::error!(!source, "failed to parse the stream"))?;
			let bytes = row
				.get::<_, Vec<u8>>(2)
				.map_err(|source| tg::error!(!source, "failed to get the bytes"))?;
			drop(rows);
			drop(statement);
			let statement = "
				delete from process_stdio
				where position = ?1;
			";
			transaction
				.execute(statement, sqlite::params![position])
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
			let chunk = tg::process::stdio::Chunk {
				bytes: Bytes::from(bytes),
				position: None,
				stream,
			};
			let event = tg::process::stdio::read::Event::Chunk(chunk);
			return Ok(Some(event));
		}
		drop(rows);
		drop(statement);

		let statement = "
			select stdin_open, stdout_open, stderr_open
			from processes
			where id = ?1;
		";
		let mut statement = transaction
			.prepare(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![id.to_string()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(Some(tg::process::stdio::read::Event::End));
		};
		let stdin_open = row
			.get::<_, Option<bool>>(0)
			.map_err(|source| tg::error!(!source, "failed to get stdin_open"))?;
		let stdout_open = row
			.get::<_, Option<bool>>(1)
			.map_err(|source| tg::error!(!source, "failed to get stdout_open"))?;
		let stderr_open = row
			.get::<_, Option<bool>>(2)
			.map_err(|source| tg::error!(!source, "failed to get stderr_open"))?;
		let closed = streams.iter().all(|stream| match stream {
			tg::process::stdio::Stream::Stdin => stdin_open == Some(false),
			tg::process::stdio::Stream::Stdout => stdout_open == Some(false),
			tg::process::stdio::Stream::Stderr => stderr_open == Some(false),
		});
		if closed {
			return Ok(Some(tg::process::stdio::read::Event::End));
		}

		Ok(None)
	}
}
