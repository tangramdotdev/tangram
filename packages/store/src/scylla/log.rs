use {indoc::indoc, std::collections::BTreeSet, tangram_client::prelude::*};

mod delete;
mod put;
mod read;

const ENTRY_KIND: i8 = 0;
const STDERR_KIND: i8 = 2;
const STDOUT_KIND: i8 = 1;

pub(super) struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	get_after: scylla::statement::prepared::PreparedStatement,
	get_at_or_before: scylla::statement::prepared::PreparedStatement,
	get_by_positions: scylla::statement::prepared::PreparedStatement,
	get_last: scylla::statement::prepared::PreparedStatement,
	put: scylla::statement::prepared::PreparedStatement,
}

impl Statements {
	pub(super) async fn new(session: &scylla::client::session::Session) -> tg::Result<Self> {
		let statement = indoc!(
			"
				delete from logs
				where process = ?;
			"
		);
		let mut delete = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log delete statement"))?;
		delete.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, combined_position, length, position, stream, stream_position, \"timestamp\"
				from logs
				where process = ? and kind = ? and position <= ?
				order by position desc
				limit 1;
			"
		);
		let mut get_at_or_before = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the log get at or before statement"
			)
		})?;
		get_at_or_before.set_consistency(scylla::statement::Consistency::One);
		get_at_or_before.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, position
				from logs
				where process = ? and kind = ? and position in ?;
			"
		);
		let mut get_by_positions = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the log get by positions statement"
			)
		})?;
		get_by_positions.set_consistency(scylla::statement::Consistency::One);
		get_by_positions.set_is_idempotent(true);

		let statement = indoc!(
			"
				select length, position
				from logs
				where process = ? and kind = ?
				order by position desc
				limit 1;
			"
		);
		let mut get_last = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log get last statement"))?;
		get_last.set_consistency(scylla::statement::Consistency::One);
		get_last.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, combined_position, length, position, stream, stream_position, \"timestamp\"
				from logs
				where process = ? and kind = ? and position > ?;
			"
		);
		let mut get_after = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log get after statement"))?;
		get_after.set_consistency(scylla::statement::Consistency::One);
		get_after.set_is_idempotent(true);
		get_after.set_page_size(128);

		let statement = indoc!(
			"
				insert into logs (
					bytes, combined_position, kind, length, position, process, stream,
					stream_position, \"timestamp\"
				)
				values (?, ?, ?, ?, ?, ?, ?, ?, ?);
			"
		);
		let mut put = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log entry statement"))?;
		put.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put.set_is_idempotent(true);
		let statements = Self {
			delete,
			get_after,
			get_at_or_before,
			get_by_positions,
			get_last,
			put,
		};

		Ok(statements)
	}
}

fn kind_for_stream(stream: tg::process::stdio::Stream) -> tg::Result<i8> {
	let kind = match stream {
		tg::process::stdio::Stream::Stderr => STDERR_KIND,
		tg::process::stdio::Stream::Stdin => {
			return Err(tg::error!("invalid stdio stream"));
		},
		tg::process::stdio::Stream::Stdout => STDOUT_KIND,
	};

	Ok(kind)
}

fn kind_for_streams(streams: &BTreeSet<tg::process::stdio::Stream>) -> tg::Result<i8> {
	if streams.is_empty() || streams.len() > 2 {
		return Err(tg::error!("invalid log streams"));
	}
	if streams.contains(&tg::process::stdio::Stream::Stdin) {
		return Err(tg::error!("invalid stdio stream"));
	}
	if streams.len() == 2 {
		return Ok(ENTRY_KIND);
	}
	let stream = streams.iter().next().copied().unwrap();

	kind_for_stream(stream)
}

fn stream_for_kind(kind: i8) -> tg::Result<tg::process::stdio::Stream> {
	let stream = match kind {
		STDERR_KIND => tg::process::stdio::Stream::Stderr,
		STDOUT_KIND => tg::process::stdio::Stream::Stdout,
		_ => return Err(tg::error!(%kind, "invalid log kind")),
	};

	Ok(stream)
}
