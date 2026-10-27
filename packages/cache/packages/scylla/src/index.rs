use {
	crate::{
		Cache,
		queue::{prepare, sequence, value},
	},
	futures::{TryStreamExt as _, future},
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
	pub async fn delete_index_queue_item_batch(
		&self,
		arg: index::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		// Delete the items concurrently, because the rows share a partition but have no batch statement.
		let indexer = arg.indexer.to_bytes();
		let sequences = arg
			.sequences
			.into_iter()
			.map(sequence)
			.collect::<tg::Result<Vec<_>>>()?;
		future::try_join_all(sequences.into_iter().map(|sequence| {
			let indexer = indexer.clone();
			async move {
				self.session
					.execute_unpaged(&self.statements.index.delete, (indexer.as_ref(), sequence))
					.await
					.map_err(|error| tg::error!(!error, "failed to delete an index queue item"))?;
				Ok::<_, tg::Error>(())
			}
		}))
		.await?;

		Ok(())
	}

	pub async fn get_index_queue_items(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Item>> {
		// Page the rows, because a range of items can exceed ScyllaDB's memory limit for unpaged queries.
		let indexer = arg.indexer.to_bytes();
		let sequence_end = sequence(arg.sequence_end)?;
		let sequence_start = sequence(arg.sequence_start)?;
		let pager = self
			.session
			.execute_iter(
				self.statements.index.get_batch.clone(),
				(indexer.as_ref(), sequence_start, sequence_end),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get index queue items"))?;
		let mut rows = pager
			.rows_stream::<(i64, Vec<u8>, i64, i64, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to get the index queue rows"))?;

		// Read the items.
		let mut output = Vec::new();
		while let Some((sequence, batch, fragment, fragments, payload)) = rows
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to read an index queue row"))?
		{
			let batch = batch
				.try_into()
				.map(index::queue::batch::Id::new)
				.map_err(|_| tg::error!("invalid index queue batch id"))?;
			let item = index::queue::Item {
				batch,
				fragment: value(fragment)?,
				fragments: value(fragments)?,
				indexer: arg.indexer.clone(),
				payload: payload.into(),
				sequence: value(sequence)?,
			};
			output.push(item);
		}

		Ok(output)
	}

	pub async fn put_index_queue_item(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		let item = arg.item;
		let batch = item.batch.value();
		let fragment_index = sequence(item.fragment)?;
		let fragments = sequence(item.fragments)?;
		let indexer = item.indexer.to_bytes();
		let sequence = sequence(item.sequence)?;
		let params = (
			batch.as_slice(),
			fragment_index,
			fragments,
			indexer.as_ref(),
			item.payload,
			sequence,
		);
		self.session
			.execute_unpaged(&self.statements.index.put, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an index queue item"))?;

		Ok(())
	}

	pub async fn try_get_index_queue_item(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Item>> {
		let indexer = arg.indexer.to_bytes();
		let sequence = sequence(arg.sequence)?;
		let result = self
			.session
			.execute_unpaged(&self.statements.index.get, (indexer.as_ref(), sequence))
			.await
			.map_err(|error| tg::error!(!error, "failed to get an index queue item"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the index queue row"))?;
		let item = result
			.maybe_first_row::<(Vec<u8>, i64, i64, Vec<u8>)>()
			.map_err(|error| tg::error!(!error, "failed to deserialize the index queue row"))?
			.map(|(batch, fragment, fragments, payload)| {
				let batch = batch
					.try_into()
					.map(index::queue::batch::Id::new)
					.map_err(|_| tg::error!("invalid index queue batch id"))?;
				let item = index::queue::Item {
					batch,
					fragment: value(fragment)?,
					fragments: value(fragments)?,
					indexer: arg.indexer.clone(),
					payload: payload.into(),
					sequence: arg.sequence,
				};

				Ok::<_, tg::Error>(item)
			})
			.transpose()?;

		Ok(item)
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
	async fn delete_index_queue_item_batch(
		&self,
		arg: tangram_cache::index::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_item_batch(arg).await
	}

	async fn get_index_queue_items(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Item>> {
		self.get_index_queue_items(arg).await
	}

	async fn put_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_item(arg).await
	}

	async fn try_get_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Item>> {
		self.try_get_index_queue_item(arg).await
	}
}
