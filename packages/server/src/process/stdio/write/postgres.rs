use {
	crate::{Session, process::stdio::MAX_UNREAD_PROCESS_STDIO_BYTES},
	bytes::Bytes,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

use super::WriteOutput;

impl Session {
	pub(crate) async fn try_write_process_stdio_postgres(
		&self,
		process_store: &db::postgres::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<WriteOutput> {
		let id = id.to_string();
		let bytes = bytes.clone();
		db::postgres::run!(process_store, |transaction| {
			Self::try_write_process_stdio_postgres_with_transaction(
				transaction,
				&id,
				stream,
				bytes.clone(),
			)
			.await
		})
		.map_err(|error| tg::error!(!error, "failed to write process stdio"))
	}

	async fn try_write_process_stdio_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		id: &str,
		stream: tg::process::stdio::Stream,
		bytes: Bytes,
	) -> tg::Result<ControlFlow<WriteOutput, db::postgres::Error>> {
		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		#[derive(db::row::Deserialize)]
		struct OpenRow {
			open: Option<bool>,
		}
		let statement = formatdoc!(
			"
				select {column} as open
				from processes
				where id = $1
				for update;
			"
		);
		let result = transaction
			.query_optional_into::<OpenRow>(statement.into(), db::params![id.to_owned()])
			.await;
		let Some(row) = crate::database::retry!(result, "failed to execute the statement") else {
			return Ok(ControlFlow::Break(WriteOutput::Closed));
		};
		if row.open != Some(true) {
			return Ok(ControlFlow::Break(WriteOutput::Closed));
		}

		let statement = "
			select coalesce(sum(octet_length(bytes)), 0)::int8
			from process_stdio
			where process = $1 and stream = $2;
		";
		let result = transaction
			.query_one_value_into::<i64>(
				statement.into(),
				db::params![id.to_owned(), stream.to_string()],
			)
			.await;
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
			values ($1, $2, $3);
		";
		let result = transaction
			.execute(
				statement.into(),
				db::params![id.to_owned(), stream.to_string(), bytes],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(WriteOutput::Written))
	}

	pub(crate) async fn try_close_process_stdio_postgres(
		&self,
		process_store: &db::postgres::Database,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<()> {
		let id = id.to_string();
		let column = match stream {
			tg::process::stdio::Stream::Stdin => "stdin_open",
			tg::process::stdio::Stream::Stdout => "stdout_open",
			tg::process::stdio::Stream::Stderr => "stderr_open",
		};
		db::postgres::run!(process_store, |transaction| {
			Self::try_close_process_stdio_postgres_with_transaction(transaction, &id, column).await
		})
		.map_err(|error| tg::error!(!error, "failed to close process stdio"))
	}

	async fn try_close_process_stdio_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		id: &str,
		column: &str,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let statement = formatdoc!(
			"
				update processes
				set {column} = false
				where id = $1;
			"
		);
		let result = transaction
			.execute(statement.into(), db::params![id.to_owned()])
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}
}
