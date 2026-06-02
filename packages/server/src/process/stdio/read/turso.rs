use {
	crate::Session,
	bytes::Bytes,
	futures::FutureExt as _,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_read_process_stdio_pipe_event_turso(
		&self,
		process_store: &db::turso::Database,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let id = id.to_string();
		let streams = streams.clone();
		process_store
			.run(|transaction| {
				let id = id.clone();
				let streams = streams.clone();
				async move {
					Self::try_read_process_stdio_pipe_event_turso_with_transaction(
						transaction,
						&id,
						&streams,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to read process stdio"))
	}

	async fn try_read_process_stdio_pipe_event_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		id: &str,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<ControlFlow<Option<tg::process::stdio::read::Event>, db::turso::Error>> {
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
		let mut params = vec![db::value::Serialize::serialize(&id.to_owned()).unwrap()];
		params.extend(
			streams
				.iter()
				.map(|stream| db::value::Serialize::serialize(&stream.to_string()).unwrap()),
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			position: i64,
			#[tangram_database(as = "db::value::FromStr")]
			stream: tg::process::stdio::Stream,
			bytes: Bytes,
		}
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		if let Some(row) = crate::database::retry!(result, "failed to execute the statement") {
			let statement = "
				delete from process_stdio
				where position = ?1;
			";
			let result = transaction
				.execute(statement.into(), db::params![row.position])
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			let chunk = tg::process::stdio::Chunk {
				bytes: row.bytes,
				position: None,
				stream: row.stream,
			};
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::Chunk(chunk),
			)));
		}

		#[derive(db::row::Deserialize)]
		struct OpenRow {
			stdin: Option<bool>,
			stdout: Option<bool>,
			stderr: Option<bool>,
		}
		let statement = "
			select stdin_open as stdin, stdout_open as stdout, stderr_open as stderr
			from processes
			where id = ?1;
		";
		let result = transaction
			.query_optional_into::<OpenRow>(statement.into(), db::params![id.to_owned()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::End,
			)));
		};
		let closed = streams.iter().all(|stream| match stream {
			tg::process::stdio::Stream::Stdin => row.stdin == Some(false),
			tg::process::stdio::Stream::Stdout => row.stdout == Some(false),
			tg::process::stdio::Stream::Stderr => row.stderr == Some(false),
		});
		if closed {
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::End,
			)));
		}

		Ok(ControlFlow::Break(None))
	}
}
