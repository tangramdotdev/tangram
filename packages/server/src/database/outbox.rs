use {
	super::{Database, Transaction},
	crate::Server,
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BatchId(u64);

#[derive(Clone, Debug)]
pub struct Item {
	pub batch: BatchId,
	pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub batch: BatchId,
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct DequeueArg {
	pub batch_size: usize,
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct EnqueueArg {
	pub batch: BatchId,
	pub items: Vec<EnqueueItem>,
	pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct EnqueueItem {
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct TryGetBatchArg {
	pub batch: Option<BatchId>,
	pub region: String,
}

#[derive(db::row::Deserialize)]
struct Row {
	batch: u64,
	payload: Bytes,
}

impl BatchId {
	#[must_use]
	pub fn new(value: u64) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> u64 {
		self.0
	}
}

impl Server {
	pub async fn enqueue_database_outbox_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: &tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if arg.is_empty() {
			return Ok(ControlFlow::Break(()));
		}
		let items = database_outbox_regions(&self.config)
			.into_iter()
			.map(|region| EnqueueItem { region })
			.collect::<Vec<_>>();
		let payload = arg.serialize()?.into();
		let result = transaction
			.query_one_value_into::<u64>(
				"update outbox_batch set next = next + 1 returning next;".into(),
				db::params![],
			)
			.await;
		let batch = crate::database::retry!(result, "failed to allocate a database outbox batch");
		let batch = BatchId::new(batch);
		let arg = EnqueueArg {
			batch,
			items,
			payload,
		};
		let p = transaction.p();
		let mut params = Vec::with_capacity(arg.items.len() * 3);
		let mut values = Vec::with_capacity(arg.items.len());
		for (index, item) in arg.items.into_iter().enumerate() {
			let offset = index * 3;
			values.push(format!(
				"({p}{}, {p}{}, {p}{})",
				offset + 1,
				offset + 2,
				offset + 3,
			));
			params.extend(db::params![
				item.region,
				arg.batch.value(),
				arg.payload.clone()
			]);
		}
		let statement = format!(
			"insert into outbox (region, batch, payload) values {};",
			values.join(", ")
		);
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to enqueue the database outbox items");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) fn spawn_publish_database_outbox_notification_task(&self) {
		let regions = database_outbox_regions(&self.config);
		tokio::spawn({
			let server = self.clone();
			async move {
				for region in regions {
					let subject = crate::indexer::database_outbox_subject();
					let target_region = (!region.is_empty()).then_some(region.as_str());
					if let Err(error) = server
						.messenger
						.publish_to_region(target_region, subject, ())
						.await
					{
						tracing::error!(%error, %region, "failed to publish a database outbox notification");
					}
				}
			}
		});
	}
}

fn database_outbox_regions(config: &crate::Config) -> BTreeSet<String> {
	let mut regions = config
		.regions
		.as_ref()
		.into_iter()
		.flatten()
		.map(|region| region.name.clone())
		.collect::<BTreeSet<_>>();
	if let Some(region) = &config.region {
		regions.insert(region.clone());
	}
	if regions.is_empty() {
		regions.insert(String::new());
	}
	regions
}

impl Database {
	pub async fn delete_outbox(&self, arg: DeleteArg) -> tg::Result<()> {
		self.run(|transaction| {
			let arg = arg.clone();
			async move { Self::delete_outbox_with_transaction(transaction, arg).await }.boxed()
		})
		.await
	}

	async fn delete_outbox_with_transaction(
		transaction: &Transaction<'_>,
		arg: DeleteArg,
	) -> tg::Result<ControlFlow<(), super::Error>> {
		let p = transaction.p();
		let statement = format!("delete from outbox where region = {p}1 and batch <= {p}2;");
		let params = db::params![arg.region, arg.batch.value()];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to delete the database outbox items");

		Ok(ControlFlow::Break(()))
	}

	pub async fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let batch_size = i64::try_from(arg.batch_size)
			.map_err(|_| tg::error!("the database outbox batch size exceeded an i64"))?;
		// Read from the write connection so a notification cannot outrun replication.
		let rows = self
			.run(|transaction| {
				let region = arg.region.clone();
				async move {
					Self::dequeue_outbox_with_transaction(transaction, &region, batch_size).await
				}
				.boxed()
			})
			.await?;
		let items = rows.into_iter().map(Item::from).collect();

		Ok(items)
	}

	async fn dequeue_outbox_with_transaction(
		transaction: &Transaction<'_>,
		region: &str,
		batch_size: i64,
	) -> tg::Result<ControlFlow<Vec<Row>, super::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select batch, payload
				from outbox
				where region = {p}1
				order by batch
				limit {p}2;
			"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![region, batch_size])
			.await;
		let rows = crate::database::retry!(result, "failed to dequeue the database outbox");

		Ok(ControlFlow::Break(rows))
	}

	pub async fn try_get_outbox_batch_at_or_before(
		&self,
		arg: TryGetBatchArg,
	) -> tg::Result<Option<BatchId>> {
		let batch = self
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let arg = arg.clone();
				async move {
					Self::try_get_outbox_batch_at_or_before_with_transaction(transaction, arg).await
				}
				.boxed()
			})
			.await?;
		let batch = batch.map(BatchId::new);

		Ok(batch)
	}

	async fn try_get_outbox_batch_at_or_before_with_transaction(
		transaction: &Transaction<'_>,
		arg: TryGetBatchArg,
	) -> tg::Result<ControlFlow<Option<u64>, super::Error>> {
		let p = transaction.p();
		let (statement, params) = if let Some(batch) = arg.batch {
			let statement = formatdoc!(
				r"
					select batch
					from outbox
					where region = {p}1 and batch <= {p}2
					order by batch desc
					limit 1;
				"
			);
			let params = db::params![arg.region, batch.value()];
			(statement, params)
		} else {
			let statement = formatdoc!(
				r"
					select batch
					from outbox
					where region = {p}1
					order by batch desc
					limit 1;
				"
			);
			let params = db::params![arg.region];
			(statement, params)
		};
		let result = transaction
			.query_optional_value_into::<u64>(statement.into(), params)
			.await;
		let batch = crate::database::retry!(result, "failed to get the database outbox batch");

		Ok(ControlFlow::Break(batch))
	}
}

impl From<Row> for Item {
	fn from(row: Row) -> Self {
		Self {
			batch: BatchId::new(row.batch),
			payload: row.payload,
		}
	}
}
