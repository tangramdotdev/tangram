use {
	crate::Session,
	bytes::Bytes,
	rusqlite as sqlite,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db},
};

impl Session {
	pub(crate) async fn try_read_process_stdio_pipe_event_sqlite(
		&self,
		process_store: &db::sqlite::Database,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let id = id.clone();
		let streams = streams.clone();
		process_store
			.run(move |transaction, _cache| {
				Self::try_read_process_stdio_pipe_event_sqlite_sync(transaction, &id, &streams)
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to read process stdio"))
	}

	fn try_read_process_stdio_pipe_event_sqlite_sync(
		transaction: &sqlite::Transaction<'_>,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<ControlFlow<Option<tg::process::stdio::read::Event>, db::sqlite::Error>> {
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
		let result = transaction
			.prepare(&statement)
			.map_err(db::sqlite::Error::from);
		let mut statement = crate::database::retry!(result, "failed to prepare the statement");
		let mut params = Vec::with_capacity(streams.len() + 1);
		params.push(id.to_string());
		params.extend(streams.iter().map(ToString::to_string));
		let mut rows = {
			let result = statement
				.query(sqlite::params_from_iter(params.iter()))
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement")
		};
		let result = rows.next().map_err(db::sqlite::Error::from);
		if let Some(row) = crate::database::retry!(result, "failed to execute the statement") {
			let result = row.get::<_, i64>(0).map_err(db::sqlite::Error::from);
			let position = crate::database::retry!(result, "failed to get the position");
			let result = row.get::<_, String>(1).map_err(db::sqlite::Error::from);
			let stream = crate::database::retry!(result, "failed to get the stream")
				.parse()
				.map_err(|error| tg::error!(!error, "failed to parse the stream"))?;
			let result = row.get::<_, Vec<u8>>(2).map_err(db::sqlite::Error::from);
			let bytes = crate::database::retry!(result, "failed to get the bytes");
			drop(rows);
			drop(statement);
			let statement = "
				delete from process_stdio
				where position = ?1;
			";
			let result = transaction
				.execute(statement, sqlite::params![position])
				.map_err(db::sqlite::Error::from);
			crate::database::retry!(result, "failed to execute the statement");
			let chunk = tg::process::stdio::Chunk {
				bytes: Bytes::from(bytes),
				position: None,
				stream,
			};
			let event = tg::process::stdio::read::Event::Chunk(chunk);
			return Ok(ControlFlow::Break(Some(event)));
		}
		drop(rows);
		drop(statement);

		let statement = "
			select stdin_open, stdout_open, stderr_open
			from processes
			where id = ?1;
		";
		let result = transaction
			.prepare(statement)
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
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::End,
			)));
		};
		let result = row
			.get::<_, Option<bool>>(0)
			.map_err(db::sqlite::Error::from);
		let stdin_open = crate::database::retry!(result, "failed to get stdin_open");
		let result = row
			.get::<_, Option<bool>>(1)
			.map_err(db::sqlite::Error::from);
		let stdout_open = crate::database::retry!(result, "failed to get stdout_open");
		let result = row
			.get::<_, Option<bool>>(2)
			.map_err(db::sqlite::Error::from);
		let stderr_open = crate::database::retry!(result, "failed to get stderr_open");
		let closed = streams.iter().all(|stream| match stream {
			tg::process::stdio::Stream::Stdin => stdin_open == Some(false),
			tg::process::stdio::Stream::Stdout => stdout_open == Some(false),
			tg::process::stdio::Stream::Stderr => stderr_open == Some(false),
		});
		if closed {
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::End,
			)));
		}

		Ok(ControlFlow::Break(None))
	}
}
