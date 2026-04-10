use {
	crate::{Server, process::stdio::MAX_UNREAD_PROCESS_STDIO_BYTES},
	bytes::Bytes,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

use super::WriteOutput;

impl Server {
	pub(crate) async fn try_write_process_stdio_postgres(
		&self,
		sandbox_store: &db::postgres::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<WriteOutput> {
		let mut connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;

		let transaction = connection
			.inner_mut()
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		let statement = formatdoc!(
			"
				select {column}
				from processes
				where id = $1
				for update;
			"
		);
		let id = id.to_string();
		let Some(row) = transaction
			.query_opt(&statement, &[&id])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(WriteOutput::Closed);
		};
		let open = row
			.try_get::<_, Option<bool>>(0)
			.map_err(|source| tg::error!(!source, "failed to get the open state"))?;
		if open != Some(true) {
			return Ok(WriteOutput::Closed);
		}

		let statement = "
			select coalesce(sum(octet_length(bytes)), 0)::int8
			from process_stdio
			where process = $1 and stream = $2;
		";
		let written_len = transaction
			.query_one(statement, &[&id, &stream.to_string()])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.try_get::<_, i64>(0)
			.map_err(|source| tg::error!(!source, "failed to get the written bytes"))?;
		let written_len = u64::try_from(written_len).unwrap();
		let bytes_len = u64::try_from(bytes.len()).unwrap();
		if written_len != 0
			&& written_len.saturating_add(bytes_len) > MAX_UNREAD_PROCESS_STDIO_BYTES
		{
			return Ok(WriteOutput::Full);
		}

		let statement = "
			insert into process_stdio (process, stream, bytes)
			values ($1, $2, $3);
		";
		transaction
			.execute(statement, &[&id, &stream.to_string(), &&bytes[..]])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(WriteOutput::Written)
	}

	pub(crate) async fn try_close_process_stdio_postgres(
		&self,
		sandbox_store: &db::postgres::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		let connection = sandbox_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a sandbox store connection"))?;
		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		let statement = formatdoc!(
			"
				update processes
				set {column} = false
				where id = $1;
			"
		);
		let id = id.to_string();
		connection
			.inner()
			.execute(&statement, &[&id])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
