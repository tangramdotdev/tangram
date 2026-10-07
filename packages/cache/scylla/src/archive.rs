use {
	crate::{
		Cache,
		queue::{prepare, sequence, value},
	},
	indoc::indoc,
	tangram_cache::archive,
	tangram_client::prelude::*,
};

pub(super) struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	get: scylla::statement::prepared::PreparedStatement,
	get_batch: scylla::statement::prepared::PreparedStatement,
	put: scylla::statement::prepared::PreparedStatement,
}

impl Cache {
	pub async fn delete_archive_queue_entry(
		&self,
		arg: archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(
				&self.statements.archive.delete,
				(indexer.as_ref(), sequence),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an archive queue entry"))?;

		Ok(())
	}

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
				&self.statements.archive.get_batch,
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
			.execute_unpaged(&self.statements.archive.put, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an archive queue entry"))?;

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
			.execute_unpaged(&self.statements.archive.get, (indexer.as_ref(), sequence))
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
}

impl Statements {
	pub(super) async fn new(
		session: &scylla::client::session::Session,
		execution_profile: Option<&scylla::client::execution_profile::ExecutionProfileHandle>,
	) -> tg::Result<Self> {
		let delete = prepare(
			session,
			"delete from archive_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let get = prepare(
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
		let get_batch = prepare(
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
		let put = prepare(
			session,
			indoc!(
				"
					insert into archive_queue (indexer, object, put, sequence)
					values (?, ?, ?, ?);
				"
			),
		)
		.await?;
		let mut statements = Self {
			delete,
			get,
			get_batch,
			put,
		};
		if let Some(handle) = execution_profile {
			for statement in [
				&mut statements.delete,
				&mut statements.get,
				&mut statements.get_batch,
				&mut statements.put,
			] {
				statement.set_execution_profile_handle(Some(handle.clone()));
			}
		}

		Ok(statements)
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_entry(arg).await
	}

	async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		self.get_archive_queue_entries(arg).await
	}

	async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_entry(arg).await
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		self.try_get_archive_queue_entry(arg).await
	}
}
