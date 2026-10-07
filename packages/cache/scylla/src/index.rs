use {
	crate::{
		Cache,
		queue::{prepare, sequence, value},
	},
	indoc::indoc,
	tangram_cache::index,
	tangram_client::prelude::*,
};

pub(super) struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	get: scylla::statement::prepared::PreparedStatement,
	get_batch: scylla::statement::prepared::PreparedStatement,
	put: scylla::statement::prepared::PreparedStatement,
}

impl Cache {
	pub async fn delete_index_queue_fragment(
		&self,
		arg: index::queue::delete::Arg,
	) -> tg::Result<()> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		self.session
			.execute_unpaged(&self.statements.index.delete, (indexer.as_ref(), sequence))
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an index queue fragment"))?;

		Ok(())
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
				&self.statements.index.get_batch,
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
			.execute_unpaged(&self.statements.index.put, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an index queue fragment"))?;

		Ok(())
	}

	pub async fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(&self.statements.index.get, (indexer.as_ref(), sequence))
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

impl Statements {
	pub(super) async fn new(
		session: &scylla::client::session::Session,
		execution_profile: Option<&scylla::client::execution_profile::ExecutionProfileHandle>,
	) -> tg::Result<Self> {
		let delete = prepare(
			session,
			"delete from index_queue where indexer = ? and sequence = ?;",
		)
		.await?;
		let get = prepare(
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
		let get_batch = prepare(
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
		let put = prepare(
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

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_fragment(arg).await
	}

	async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		self.get_index_queue_fragments(arg).await
	}

	async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_fragment(arg).await
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		self.try_get_index_queue_fragment(arg).await
	}
}
