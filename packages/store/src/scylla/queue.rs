use {
	super::Store,
	crate::{archive, index},
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

pub(super) struct Statements {
	delete_archive: scylla::statement::prepared::PreparedStatement,
	delete_index: scylla::statement::prepared::PreparedStatement,
	get_archive: scylla::statement::prepared::PreparedStatement,
	get_archive_batch: scylla::statement::prepared::PreparedStatement,
	get_index: scylla::statement::prepared::PreparedStatement,
	get_index_batch: scylla::statement::prepared::PreparedStatement,
	put_archive: scylla::statement::prepared::PreparedStatement,
	put_index: scylla::statement::prepared::PreparedStatement,
}

impl Statements {
	pub(super) async fn new(session: &scylla::client::session::Session) -> tg::Result<Self> {
		let delete_archive = prepare(
			session,
			"delete from archive_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let delete_index = prepare(
			session,
			"delete from index_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let get_archive = prepare(
			session,
			indoc!(
				"
					select object, put
					from archive_queue
					where indexer = ? and sequence = ?;
				"
			),
		)
		.await?;
		let get_archive_batch = prepare(
			session,
			indoc!(
				"
					select sequence, object, put
					from archive_queue
					where indexer = ? and sequence >= ? and sequence < ?;
				"
			),
		)
		.await?;
		let get_index = prepare(
			session,
			indoc!(
				r#"
					select "batch", fragment, fragments, payload
					from index_queue
					where indexer = ? and sequence = ?;
				"#
			),
		)
		.await?;
		let get_index_batch = prepare(
			session,
			indoc!(
				r#"
					select sequence, "batch", fragment, fragments, payload
					from index_queue
					where indexer = ? and sequence >= ? and sequence < ?;
				"#
			),
		)
		.await?;
		let put_archive = prepare(
			session,
			indoc!(
				"
					insert into archive_queue (indexer, object, put, sequence)
					values (?, ?, ?, ?);
				"
			),
		)
		.await?;
		let put_index = prepare(
			session,
			indoc!(
				r#"
					insert into index_queue (
						"batch", fragment, fragments, indexer, payload, sequence
					) values (?, ?, ?, ?, ?, ?);
				"#
			),
		)
		.await?;
		let statements = Self {
			delete_archive,
			delete_index,
			get_archive,
			get_archive_batch,
			get_index,
			get_index_batch,
			put_archive,
			put_index,
		};

		Ok(statements)
	}
}

impl Store {
	pub async fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Entry>> {
		let indexer = arg.indexer.to_bytes();
		let sequence_end = sequence(arg.sequence_end)?;
		let sequence_start = sequence(arg.sequence_start)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_archive_batch,
				(indexer.as_ref(), sequence_start, sequence_end),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get archive queue entries"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the archive queue rows"))?;
		let entries = result
			.rows::<(i64, Vec<u8>, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to iterate the archive queue rows"))?
			.map(|row| {
				let (sequence, object, put) = row.map_err(|error| {
					tg::error!(!error, "failed to deserialize an archive queue row")
				})?;
				let object = tg::object::Id::from_slice(&object)?;
				let put = put
					.try_into()
					.map_err(|_| tg::error!("invalid archive queue put"))?;
				let entry = archive::queue::Entry {
					indexer: arg.indexer.clone(),
					object,
					put,
					sequence: value(sequence)?,
				};

				Ok(entry)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(entries)
	}

	pub async fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Fragment>> {
		let indexer = arg.indexer.to_bytes();
		let sequence_end = sequence(arg.sequence_end)?;
		let sequence_start = sequence(arg.sequence_start)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_index_batch,
				(indexer.as_ref(), sequence_start, sequence_end),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get index queue fragments"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the index queue rows"))?;
		let fragments = result
			.rows::<(i64, Vec<u8>, i64, i64, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to iterate the index queue rows"))?
			.map(|row| {
				let (sequence, batch, fragment, fragments, payload) = row.map_err(|error| {
					tg::error!(!error, "failed to deserialize an index queue row")
				})?;
				let batch = batch
					.try_into()
					.map(index::queue::batch::Id::new)
					.map_err(|_| tg::error!("invalid index queue batch id"))?;
				let fragment = index::queue::Fragment {
					batch,
					fragment: value(fragment)?,
					fragments: value(fragments)?,
					indexer: arg.indexer.clone(),
					payload: payload.into(),
					sequence: value(sequence)?,
				};

				Ok(fragment)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(fragments)
	}

	pub async fn delete_archive_queue_entry(
		&self,
		arg: archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(
				&self.statements.queue.delete_archive,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an archive queue entry"))?;

		Ok(())
	}

	pub async fn delete_index_queue_fragment(
		&self,
		arg: index::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(
				&self.statements.queue.delete_index,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an index queue fragment"))?;

		Ok(())
	}

	pub async fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) -> tg::Result<()> {
		let entry = arg.entry;
		let indexer = entry.indexer.to_bytes();
		let object = entry.object.to_bytes();
		let sequence = sequence(entry.sequence)?;
		let params = (
			indexer.as_ref(),
			object.as_ref(),
			entry.put.as_slice(),
			sequence,
		);
		self.session
			.execute_unpaged(&self.statements.queue.put_archive, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an archive queue entry"))?;

		Ok(())
	}

	pub async fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		let fragment = arg.fragment;
		let batch = fragment.batch.value();
		let fragment_index = sequence(fragment.fragment)?;
		let fragments = sequence(fragment.fragments)?;
		let indexer = fragment.indexer.to_bytes();
		let sequence = sequence(fragment.sequence)?;
		let params = (
			batch.as_slice(),
			fragment_index,
			fragments,
			indexer.as_ref(),
			fragment.payload,
			sequence,
		);
		self.session
			.execute_unpaged(&self.statements.queue.put_index, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an index queue fragment"))?;

		Ok(())
	}

	pub async fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Entry>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_archive,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an archive queue entry"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the archive queue row"))?;
		let entry = result
			.maybe_first_row::<(Vec<u8>, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to deserialize the archive queue row"))?
			.map(|(object, put)| {
				let object = tg::object::Id::from_slice(&object)?;
				let put = put
					.try_into()
					.map_err(|_| tg::error!("invalid archive queue put"))?;
				let entry = archive::queue::Entry {
					indexer: arg.indexer.clone(),
					object,
					put,
					sequence: arg.sequence,
				};

				Ok::<_, tg::Error>(entry)
			})
			.transpose()?;

		Ok(entry)
	}

	pub async fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_index,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an index queue fragment"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the index queue row"))?;
		let fragment = result
			.maybe_first_row::<(Vec<u8>, i64, i64, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to deserialize the index queue row"))?
			.map(|(batch, fragment, fragments, payload)| {
				let batch = batch
					.try_into()
					.map(index::queue::batch::Id::new)
					.map_err(|_| tg::error!("invalid index queue batch id"))?;
				let fragment = index::queue::Fragment {
					batch,
					fragment: value(fragment)?,
					fragments: value(fragments)?,
					indexer: arg.indexer.clone(),
					payload: payload.into(),
					sequence: arg.sequence,
				};

				Ok::<_, tg::Error>(fragment)
			})
			.transpose()?;

		Ok(fragment)
	}
}

async fn prepare(
	session: &scylla::client::session::Session,
	statement: &str,
) -> tg::Result<scylla::statement::prepared::PreparedStatement> {
	let mut statement = session
		.prepare(statement)
		.await
		.map_err(|error| tg::error!(!error, "failed to prepare an object queue statement"))?;
	statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
	statement.set_is_idempotent(true);

	Ok(statement)
}

fn sequence(value: u64) -> tg::Result<i64> {
	value
		.to_i64()
		.ok_or_else(|| tg::error!("the object queue sequence exceeded an i64"))
}

fn value(sequence: i64) -> tg::Result<u64> {
	sequence
		.to_u64()
		.ok_or_else(|| tg::error!("the object queue sequence was negative"))
}
