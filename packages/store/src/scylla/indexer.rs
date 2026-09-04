use {
	super::Store, crate::indexer, indoc::indoc, num::ToPrimitive as _, tangram_client::prelude::*,
};

const PARTITION: i8 = 0;

pub(super) struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	get: scylla::statement::prepared::PreparedStatement,
	list: scylla::statement::prepared::PreparedStatement,
	put: scylla::statement::prepared::PreparedStatement,
	update_archive_read_sequence: scylla::statement::prepared::PreparedStatement,
	update_archive_write_sequence: scylla::statement::prepared::PreparedStatement,
	update_available: scylla::statement::prepared::PreparedStatement,
	update_index_read_sequence: scylla::statement::prepared::PreparedStatement,
	update_index_write_sequence: scylla::statement::prepared::PreparedStatement,
}

#[derive(scylla::DeserializeRow)]
struct Row {
	archive_read_sequence: i64,
	archive_write_sequence: i64,
	available: bool,
	id: Vec<u8>,
	index_read_sequence: i64,
	index_write_sequence: i64,
}

impl Statements {
	pub(super) async fn new(session: &scylla::client::session::Session) -> tg::Result<Self> {
		let delete = prepare(
			session,
			indoc!(
				"
					delete from indexers
					where partition = ? and id = ?;
				"
			),
		)
		.await?;
		let get = prepare(
			session,
			indoc!(
				"
					select archive_read_sequence, archive_write_sequence, available, id,
						index_read_sequence, index_write_sequence
					from indexers
					where partition = ? and id = ?;
				"
			),
		)
		.await?;
		let list = prepare(
			session,
			indoc!(
				"
					select archive_read_sequence, archive_write_sequence, available, id,
						index_read_sequence, index_write_sequence
					from indexers
					where partition = ?;
				"
			),
		)
		.await?;
		let put = prepare(
			session,
			indoc!(
				"
					insert into indexers (
						archive_read_sequence, archive_write_sequence, available, id,
						index_read_sequence, index_write_sequence, partition
					) values (?, ?, ?, ?, ?, ?, ?);
				"
			),
		)
		.await?;
		let update_archive_read_sequence = prepare(
			session,
			"update indexers set archive_read_sequence = ? where partition = ? and id = ?;",
		)
		.await?;
		let update_archive_write_sequence = prepare(
			session,
			"update indexers set archive_write_sequence = ? where partition = ? and id = ?;",
		)
		.await?;
		let update_available = prepare(
			session,
			"update indexers set available = ? where partition = ? and id = ?;",
		)
		.await?;
		let update_index_read_sequence = prepare(
			session,
			"update indexers set index_read_sequence = ? where partition = ? and id = ?;",
		)
		.await?;
		let update_index_write_sequence = prepare(
			session,
			"update indexers set index_write_sequence = ? where partition = ? and id = ?;",
		)
		.await?;
		let statements = Self {
			delete,
			get,
			list,
			put,
			update_archive_read_sequence,
			update_archive_write_sequence,
			update_available,
			update_index_read_sequence,
			update_index_write_sequence,
		};

		Ok(statements)
	}
}

impl Store {
	pub async fn delete_indexer(&self, arg: indexer::delete::Arg) -> tg::Result<()> {
		let id = arg.id.to_bytes();
		self.session
			.execute_unpaged(&self.statements.indexer.delete, (PARTITION, id.as_ref()))
			.await
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to delete the indexer"))?;

		Ok(())
	}

	pub async fn get_indexers(&self) -> tg::Result<Vec<indexer::Indexer>> {
		let result = self
			.session
			.execute_unpaged(&self.statements.indexer.list, (PARTITION,))
			.await
			.map_err(|error| tg::error!(!error, "failed to list the indexers"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the indexer rows"))?;
		let indexers = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate the indexer rows"))?
			.map(|row| {
				let row = row
					.map_err(|error| tg::error!(!error, "failed to deserialize an indexer row"))?;
				convert(&row)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(indexers)
	}

	pub async fn put_indexer(&self, arg: indexer::put::Arg) -> tg::Result<()> {
		let indexer = arg.indexer;
		let archive_read_sequence = sequence(indexer.archive_read_sequence)?;
		let archive_write_sequence = sequence(indexer.archive_write_sequence)?;
		let id = indexer.id.to_bytes();
		let index_read_sequence = sequence(indexer.index_read_sequence)?;
		let index_write_sequence = sequence(indexer.index_write_sequence)?;
		let params = (
			archive_read_sequence,
			archive_write_sequence,
			indexer.available,
			id.as_ref(),
			index_read_sequence,
			index_write_sequence,
			PARTITION,
		);
		self.session
			.execute_unpaged(&self.statements.indexer.put, params)
			.await
			.map_err(|error| tg::error!(!error, id = %indexer.id, "failed to put the indexer"))?;

		Ok(())
	}

	pub async fn try_get_indexer(
		&self,
		arg: indexer::get::Arg,
	) -> tg::Result<Option<indexer::Indexer>> {
		let id = arg.id.to_bytes();
		let result = self
			.session
			.execute_unpaged(&self.statements.indexer.get, (PARTITION, id.as_ref()))
			.await
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the indexer"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the indexer row"))?;
		let indexer = result
			.maybe_first_row::<Row>()
			.map_err(|error| tg::error!(!error, "failed to deserialize the indexer row"))?
			.map(|row| convert(&row))
			.transpose()?;

		Ok(indexer)
	}

	pub async fn update_indexer(&self, arg: indexer::update::Arg) -> tg::Result<()> {
		let id = arg.id.to_bytes();
		let result = match arg.value {
			indexer::update::Value::ArchiveReadSequence(value) => {
				let value = sequence(value)?;
				self.session
					.execute_unpaged(
						&self.statements.indexer.update_archive_read_sequence,
						(value, PARTITION, id.as_ref()),
					)
					.await
			},
			indexer::update::Value::ArchiveWriteSequence(value) => {
				let value = sequence(value)?;
				self.session
					.execute_unpaged(
						&self.statements.indexer.update_archive_write_sequence,
						(value, PARTITION, id.as_ref()),
					)
					.await
			},
			indexer::update::Value::Available(value) => {
				self.session
					.execute_unpaged(
						&self.statements.indexer.update_available,
						(value, PARTITION, id.as_ref()),
					)
					.await
			},
			indexer::update::Value::IndexReadSequence(value) => {
				let value = sequence(value)?;
				self.session
					.execute_unpaged(
						&self.statements.indexer.update_index_read_sequence,
						(value, PARTITION, id.as_ref()),
					)
					.await
			},
			indexer::update::Value::IndexWriteSequence(value) => {
				let value = sequence(value)?;
				self.session
					.execute_unpaged(
						&self.statements.indexer.update_index_write_sequence,
						(value, PARTITION, id.as_ref()),
					)
					.await
			},
		};
		result.map_err(|error| tg::error!(!error, id = %arg.id, "failed to update the indexer"))?;

		Ok(())
	}
}

async fn prepare(
	session: &scylla::client::session::Session,
	statement: &str,
) -> tg::Result<scylla::statement::prepared::PreparedStatement> {
	let mut statement = session
		.prepare(statement)
		.await
		.map_err(|error| tg::error!(!error, "failed to prepare an indexer statement"))?;
	statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
	statement.set_is_idempotent(true);

	Ok(statement)
}

fn convert(row: &Row) -> tg::Result<indexer::Indexer> {
	let archive_read_sequence = value(row.archive_read_sequence)?;
	let archive_write_sequence = value(row.archive_write_sequence)?;
	let id = tg::indexer::Id::from_slice(&row.id)?;
	let index_read_sequence = value(row.index_read_sequence)?;
	let index_write_sequence = value(row.index_write_sequence)?;
	let indexer = indexer::Indexer {
		archive_read_sequence,
		archive_write_sequence,
		available: row.available,
		id,
		index_read_sequence,
		index_write_sequence,
	};

	Ok(indexer)
}

fn sequence(value: u64) -> tg::Result<i64> {
	value
		.to_i64()
		.ok_or_else(|| tg::error!("the indexer sequence exceeded an i64"))
}

fn value(sequence: i64) -> tg::Result<u64> {
	sequence
		.to_u64()
		.ok_or_else(|| tg::error!("the indexer sequence was negative"))
}
