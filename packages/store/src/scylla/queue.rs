use {
	super::Store, crate::object, indoc::indoc, num::ToPrimitive as _, tangram_client::prelude::*,
};

pub(super) struct Statements {
	delete_archive: scylla::statement::prepared::PreparedStatement,
	delete_index: scylla::statement::prepared::PreparedStatement,
	get_archive: scylla::statement::prepared::PreparedStatement,
	get_index: scylla::statement::prepared::PreparedStatement,
	put_archive: scylla::statement::prepared::PreparedStatement,
	put_index: scylla::statement::prepared::PreparedStatement,
}

impl Statements {
	pub(super) async fn new(session: &scylla::client::session::Session) -> tg::Result<Self> {
		let delete_archive = prepare(
			session,
			"delete from object_archive_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let delete_index = prepare(
			session,
			"delete from object_index_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let get_archive = prepare(
			session,
			indoc!(
				"
					select object, put
					from object_archive_queue
					where indexer = ? and sequence = ?;
				"
			),
		)
		.await?;
		let get_index = prepare(
			session,
			indoc!(
				r#"
					select "batch", fragment, fragments, payload
					from object_index_queue
					where indexer = ? and sequence = ?;
				"#
			),
		)
		.await?;
		let put_archive = prepare(
			session,
			indoc!(
				"
					insert into object_archive_queue (indexer, object, put, sequence)
					values (?, ?, ?, ?);
				"
			),
		)
		.await?;
		let put_index = prepare(
			session,
			indoc!(
				r#"
					insert into object_index_queue (
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
			get_index,
			put_archive,
			put_index,
		};

		Ok(statements)
	}
}

impl Store {
	pub async fn delete_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(
				&self.statements.queue.delete_archive,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete an object archive queue entry")
			})?;

		Ok(())
	}

	pub async fn delete_object_index_queue_fragment(
		&self,
		arg: object::index::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(
				&self.statements.queue.delete_index,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to delete an object index queue fragment")
			})?;

		Ok(())
	}

	pub async fn put_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::put::Arg,
	) -> tg::Result<()> {
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
			.map_err(|error| tg::error!(!error, "failed to put an object archive queue entry"))?;

		Ok(())
	}

	pub async fn put_object_index_queue_fragment(
		&self,
		arg: object::index::queue::put::Arg,
	) -> tg::Result<()> {
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
			.map_err(|error| tg::error!(!error, "failed to put an object index queue fragment"))?;

		Ok(())
	}

	pub async fn try_get_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::get::Arg,
	) -> tg::Result<Option<object::archive::queue::Entry>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_archive,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an object archive queue entry"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the object archive queue row"))?;
		let entry = result
			.maybe_first_row::<(Vec<u8>, Vec<u8>)>()
			.map_err(|error| {
				tg::error!(!error, "failed to deserialize the object archive queue row")
			})?
			.map(|(object, put)| {
				let object = tg::object::Id::from_slice(&object)?;
				let put = put
					.try_into()
					.map_err(|_| tg::error!("invalid object archive queue put"))?;
				let entry = object::archive::queue::Entry {
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

	pub async fn try_get_object_index_queue_fragment(
		&self,
		arg: object::index::queue::get::Arg,
	) -> tg::Result<Option<object::index::queue::Fragment>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(
				&self.statements.queue.get_index,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get an object index queue fragment"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the object index queue row"))?;
		let fragment = result
			.maybe_first_row::<(Vec<u8>, i64, i64, Vec<u8>)>()
			.map_err(|error| {
				tg::error!(!error, "failed to deserialize the object index queue row")
			})?
			.map(|(batch, fragment, fragments, payload)| {
				let batch = batch
					.try_into()
					.map(object::index::queue::batch::Id::new)
					.map_err(|_| tg::error!("invalid object index queue batch id"))?;
				let fragment = object::index::queue::Fragment {
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
