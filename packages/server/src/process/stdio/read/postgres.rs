use {
	crate::Session,
	bytes::Bytes,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_read_process_stdio_pipe_event_postgres(
		&self,
		process_store: &db::postgres::Database,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let id = id.to_string();
		let streams = streams.iter().map(ToString::to_string).collect::<Vec<_>>();
		db::postgres::run!(process_store, |transaction| {
			Self::try_read_process_stdio_pipe_event_postgres_with_transaction(
				transaction,
				&id,
				&streams,
			)
			.await
		})
		.map_err(|error| tg::error!(!error, "failed to read process stdio"))
	}

	async fn try_read_process_stdio_pipe_event_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		id: &str,
		streams: &[String],
	) -> tg::Result<ControlFlow<Option<tg::process::stdio::read::Event>, db::postgres::Error>> {
		let placeholders = (0..streams.len())
			.map(|index| format!("${}", index + 2))
			.collect::<Vec<_>>()
			.join(", ");
		#[derive(db::row::Deserialize)]
		struct Row {
			position: i64,
			bytes: Bytes,
			#[tangram_database(as = "db::value::FromStr")]
			stream: tg::process::stdio::Stream,
		}
		let statement = "
			with candidate as (
				select position, stream, bytes
				from process_stdio
				where process = $1 and stream in ({placeholders})
				order by position
				limit 1
				for update skip locked
			),
			deleted as (
				delete from process_stdio
				where position in (select position from candidate)
				returning position, stream, bytes
			)
			select position, stream, bytes
			from deleted;
		";
		let statement = statement.replace("{placeholders}", &placeholders);
		let mut params = vec![db::value::Serialize::serialize(&id.to_owned()).unwrap()];
		params.extend(
			streams
				.iter()
				.map(|stream| db::value::Serialize::serialize(stream).unwrap()),
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), params)
			.await;
		if let Some(row) = crate::database::retry!(result, "failed to execute the statement") {
			let _ = row.position;
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::Chunk(tg::process::stdio::Chunk {
					bytes: row.bytes,
					position: None,
					stream: row.stream,
				}),
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
			where id = $1;
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
		let closed = streams.iter().all(|stream| match stream.as_str() {
			"stdin" => row.stdin == Some(false),
			"stdout" => row.stdout == Some(false),
			"stderr" => row.stderr == Some(false),
			_ => false,
		});
		if closed {
			return Ok(ControlFlow::Break(Some(
				tg::process::stdio::read::Event::End,
			)));
		}

		Ok(ControlFlow::Break(None))
	}
}
