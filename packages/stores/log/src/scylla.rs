use {
	crate::{DeleteArg, Entry, PutArg, ReadArg},
	futures::FutureExt as _,
	indoc::indoc,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

mod delete;
mod put;
mod read;

const ENTRY_KIND: i8 = 0;
const STDERR_KIND: i8 = 2;
const STDOUT_KIND: i8 = 1;

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub connections: Option<usize>,
	pub keyspace: String,
	pub password: Option<String>,
	pub username: Option<String>,
}

pub struct Store {
	session: scylla::client::session::Session,
	statements: Statements,
}

struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	get_after: scylla::statement::prepared::PreparedStatement,
	get_at_or_before: scylla::statement::prepared::PreparedStatement,
	get_by_positions: scylla::statement::prepared::PreparedStatement,
	get_last: scylla::statement::prepared::PreparedStatement,
	put_entry: scylla::statement::prepared::PreparedStatement,
	put_stream_position: scylla::statement::prepared::PreparedStatement,
}

impl Store {
	pub async fn new(config: &Config) -> tg::Result<Self> {
		// Create the session.
		let mut builder =
			scylla::client::session_builder::SessionBuilder::new().known_node(&config.addr);
		if let (Some(username), Some(password)) = (&config.username, &config.password) {
			builder = builder.user(username, password);
		}
		if let Some(connections) = config.connections.and_then(std::num::NonZeroUsize::new) {
			builder = builder.pool_size(scylla::client::PoolSize::PerHost(connections));
		}
		let session = builder.build().boxed().await.map_err(
			|error| tg::error!(!error, addr = %config.addr, "failed to build the session"),
		)?;
		session.use_keyspace(&config.keyspace, true).await.map_err(
			|error| tg::error!(!error, keyspace = %config.keyspace, "failed to use the keyspace"),
		)?;

		// Prepare the delete statement.
		let statement = indoc!(
			"
				delete from logs
				where process = ?;
			"
		);
		let mut delete = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the delete statement"))?;
		delete.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete.set_is_idempotent(true);

		// Prepare the read statements.
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
			tg::error!(!error, "failed to prepare the get at or before statement")
		})?;
		get_at_or_before.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_at_or_before.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, position
				from logs
				where process = ? and kind = ? and position in ?;
			"
		);
		let mut get_by_positions = session.prepare(statement).await.map_err(|error| {
			tg::error!(!error, "failed to prepare the get by positions statement")
		})?;
		get_by_positions.set_consistency(scylla::statement::Consistency::LocalQuorum);
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
			.map_err(|error| tg::error!(!error, "failed to prepare the get last statement"))?;
		get_last.set_consistency(scylla::statement::Consistency::LocalQuorum);
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
			.map_err(|error| tg::error!(!error, "failed to prepare the get after statement"))?;
		get_after.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_after.set_is_idempotent(true);
		get_after.set_page_size(128);

		// Prepare the write statements.
		let statement = indoc!(
			"
				insert into logs (
					bytes, combined_position, kind, length, position, process, stream,
					stream_position, \"timestamp\"
				)
				values (?, ?, ?, ?, ?, ?, ?, ?, ?);
			"
		);
		let mut put_entry = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the put entry statement"))?;
		put_entry.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_entry.set_is_idempotent(true);

		let statement = indoc!(
			"
				insert into logs (
					combined_position, kind, length, position, process, stream, stream_position,
					\"timestamp\"
				)
				values (?, ?, ?, ?, ?, ?, ?, ?);
			"
		);
		let mut put_stream_position = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the put stream position statement"
			)
		})?;
		put_stream_position.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_stream_position.set_is_idempotent(true);

		let statements = Statements {
			delete,
			get_after,
			get_at_or_before,
			get_by_positions,
			get_last,
			put_entry,
			put_stream_position,
		};
		let store = Self {
			session,
			statements,
		};

		Ok(store)
	}
}

impl crate::Store for Store {
	async fn try_read(&self, arg: ReadArg) -> tg::Result<Vec<Entry<'static>>> {
		self.try_read(arg).await
	}

	async fn try_get_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		self.try_get_length(id, streams).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg).await
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
