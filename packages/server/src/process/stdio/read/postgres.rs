use {
	crate::Server,
	bytes::Bytes,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_read_process_stdio_pipe_event_postgres(
		&self,
		sandbox_store: &db::postgres::Database,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let mut connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			position: i64,
			bytes: Vec<u8>,
			#[tangram_database(as = "db::postgres::value::FromStr")]
			stream: tg::process::stdio::Stream,
		}
		let statement = "
			with candidate as (
				select position, stream, bytes
				from process_stdio
				where process = $1 and stream = any($2::text[])
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
		let id = id.to_string();
		let streams = streams.iter().map(ToString::to_string).collect::<Vec<_>>();
		if let Some(row) = transaction
			.query_opt(statement, &[&id, &streams])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		{
			let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let _ = row.position;
			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;
			return Ok(Some(tg::process::stdio::read::Event::Chunk(
				tg::process::stdio::Chunk {
					bytes: Bytes::from(row.bytes),
					position: None,
					stream: row.stream,
				},
			)));
		}

		#[derive(db::postgres::row::Deserialize)]
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
		let row = transaction
			.query_opt(statement, &[&id])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| {
				<OpenRow as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.transpose()?;
		let Some(row) = row else {
			return Ok(Some(tg::process::stdio::read::Event::End));
		};
		let closed = streams.iter().all(|stream| match stream.as_str() {
			"stdin" => row.stdin == Some(false),
			"stdout" => row.stdout == Some(false),
			"stderr" => row.stderr == Some(false),
			_ => false,
		});
		if closed {
			return Ok(Some(tg::process::stdio::read::Event::End));
		}

		Ok(None)
	}
}
