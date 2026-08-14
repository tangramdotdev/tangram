use {
	super::{Database, Transaction},
	crate::Server,
	bytes::Bytes,
	futures::{FutureExt as _, future::BoxFuture},
	indoc::formatdoc,
	std::{collections::BTreeSet, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_messenger::Messenger as _,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id([u8; 16]);

#[derive(Clone, Debug)]
pub struct Item {
	pub id: Id,
	pub partition: u64,
	pub payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub struct Key {
	pub id: Id,
	pub partition: u64,
}

#[derive(Clone, Debug)]
pub struct DeleteArg {
	pub keys: Vec<Key>,
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct DequeueArg {
	pub batch_size: usize,
	pub partition_end: u64,
	pub partition_start: u64,
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct EnqueueArg {
	pub id: Id,
	pub items: Vec<EnqueueItem>,
	pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct EnqueueItem {
	pub partition: u64,
	pub region: String,
}

#[derive(Clone, Debug)]
pub struct TryGetIdArg {
	pub id: Option<Id>,
	pub partition_end: u64,
	pub partition_start: u64,
	pub region: String,
}

#[derive(db::row::Deserialize)]
struct Row {
	id: Bytes,
	partition: u64,
	payload: Bytes,
}

impl Id {
	#[must_use]
	pub fn new(value: [u8; 16]) -> Self {
		Self(value)
	}

	#[must_use]
	pub fn value(self) -> [u8; 16] {
		self.0
	}
}

impl Server {
	pub async fn run_database_outbox_transaction<F, T, E>(&self, f: F) -> tg::Result<T>
	where
		for<'a, 'b> F: Fn(
				&'a Transaction<'b>,
				u64,
			) -> BoxFuture<'a, std::result::Result<ControlFlow<T, crate::database::Error>, E>>
			+ Sync,
		T: Send + 'static,
		E: Into<crate::database::Error> + Send + 'static,
	{
		let config = self.config.database.outbox();
		let partition = rand::random_range(0..config.partition_total);
		let output = self
			.database
			.run(|transaction| f(transaction, partition))
			.await?;
		self.spawn_publish_database_outbox_notification_task(partition);

		Ok(output)
	}

	pub async fn enqueue_database_outbox_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		database_outbox_partition: u64,
		arg: &tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if arg.is_empty() {
			return Ok(ControlFlow::Break(()));
		}
		let mut regions = self
			.config
			.regions
			.as_ref()
			.into_iter()
			.flatten()
			.map(|region| region.name.clone())
			.collect::<BTreeSet<_>>();
		if let Some(region) = &self.config.region {
			regions.insert(region.clone());
		}
		if regions.is_empty() {
			regions.insert(String::new());
		}
		let items = regions
			.into_iter()
			.map(|region| EnqueueItem {
				partition: database_outbox_partition,
				region,
			})
			.collect::<Vec<_>>();
		let id = Id::new(uuid::Uuid::now_v7().into_bytes());
		let payload = arg.serialize()?.into();
		let arg = EnqueueArg { id, items, payload };
		let p = transaction.p();
		let mut params = Vec::with_capacity(arg.items.len() * 4);
		let mut values = Vec::with_capacity(arg.items.len());
		let id = Bytes::copy_from_slice(&arg.id.value());
		for (index, item) in arg.items.into_iter().enumerate() {
			let offset = index * 4;
			values.push(format!(
				"({p}{}, {p}{}, {p}{}, {p}{})",
				offset + 1,
				offset + 2,
				offset + 3,
				offset + 4,
			));
			let partition = partition(item.partition)?;
			params.extend(db::params![
				item.region,
				partition,
				id.clone(),
				arg.payload.clone()
			]);
		}
		let statement = format!(
			"insert into outbox (region, partition, id, payload) values {};",
			values.join(", ")
		);
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to enqueue the database outbox items");

		Ok(ControlFlow::Break(()))
	}

	fn spawn_publish_database_outbox_notification_task(&self, partition: u64) {
		let subject = crate::indexer::database_outbox_subject(partition);
		tokio::spawn({
			let server = self.clone();
			async move {
				if let Err(error) = server.messenger.publish(subject, ()).await {
					tracing::error!(%error, %partition, "failed to publish a database outbox notification");
				}
			}
		});
	}
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
		if arg.keys.is_empty() {
			return Ok(ControlFlow::Break(()));
		}
		let p = transaction.p();
		let mut params = db::params![arg.region];
		let mut predicates = Vec::with_capacity(arg.keys.len());
		for (index, key) in arg.keys.into_iter().enumerate() {
			let offset = index * 2 + 2;
			predicates.push(format!(
				"(partition = {p}{offset} and id = {p}{})",
				offset + 1
			));
			let partition = partition(key.partition)?;
			let id = Bytes::copy_from_slice(&key.id.value());
			params.extend(db::params![partition, id]);
		}
		let statement = format!(
			"delete from outbox where region = {p}1 and ({});",
			predicates.join(" or ")
		);
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to delete the database outbox items");

		Ok(ControlFlow::Break(()))
	}

	pub async fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let partition_end = partition(arg.partition_end)?;
		let partition_start = partition(arg.partition_start)?;
		let batch_size = i64::try_from(arg.batch_size)
			.map_err(|_| tg::error!("the database outbox batch size exceeded an i64"))?;
		let rows = self
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let region = arg.region.clone();
				async move {
					Self::dequeue_outbox_with_transaction(
						transaction,
						&region,
						partition_start,
						partition_end,
						batch_size,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let items = rows
			.into_iter()
			.map(Item::try_from)
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(items)
	}

	async fn dequeue_outbox_with_transaction(
		transaction: &Transaction<'_>,
		region: &str,
		partition_start: i64,
		partition_end: i64,
		batch_size: i64,
	) -> tg::Result<ControlFlow<Vec<Row>, super::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				select id, partition, payload
				from outbox
				where region = {p}1 and partition >= {p}2 and partition < {p}3
				order by partition, id
				limit {p}4;
			"
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![region, partition_start, partition_end, batch_size],
			)
			.await;
		let rows = crate::database::retry!(result, "failed to dequeue the database outbox");

		Ok(ControlFlow::Break(rows))
	}

	pub async fn try_get_outbox_id_at_or_before(&self, arg: TryGetIdArg) -> tg::Result<Option<Id>> {
		let partition_end = partition(arg.partition_end)?;
		let partition_start = partition(arg.partition_start)?;
		let id = self
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let arg = arg.clone();
				async move {
					Self::try_get_outbox_id_at_or_before_with_transaction(
						transaction,
						arg,
						partition_start,
						partition_end,
					)
					.await
				}
				.boxed()
			})
			.await?;
		let id = id.as_ref().map(decode_id).transpose()?;

		Ok(id)
	}

	async fn try_get_outbox_id_at_or_before_with_transaction(
		transaction: &Transaction<'_>,
		arg: TryGetIdArg,
		partition_start: i64,
		partition_end: i64,
	) -> tg::Result<ControlFlow<Option<Bytes>, super::Error>> {
		let p = transaction.p();
		let (statement, params) = if let Some(id) = arg.id {
			let statement = formatdoc!(
				r"
					select id
					from outbox
					where region = {p}1 and partition >= {p}2 and partition < {p}3 and id <= {p}4
					order by id desc
					limit 1;
				"
			);
			let id = Bytes::copy_from_slice(&id.value());
			let params = db::params![arg.region, partition_start, partition_end, id];
			(statement, params)
		} else {
			let statement = formatdoc!(
				r"
					select id
					from outbox
					where region = {p}1 and partition >= {p}2 and partition < {p}3
					order by id desc
					limit 1;
				"
			);
			let params = db::params![arg.region, partition_start, partition_end];
			(statement, params)
		};
		let result = transaction
			.query_optional_value_into::<Bytes>(statement.into(), params)
			.await;
		let id = crate::database::retry!(result, "failed to get the database outbox id");

		Ok(ControlFlow::Break(id))
	}
}

impl TryFrom<Row> for Item {
	type Error = tg::Error;

	fn try_from(row: Row) -> Result<Self, Self::Error> {
		let id = decode_id(&row.id)?;
		Ok(Self {
			id,
			partition: row.partition,
			payload: row.payload,
		})
	}
}

fn decode_id(bytes: &Bytes) -> tg::Result<Id> {
	let value = bytes
		.as_ref()
		.try_into()
		.map_err(|_| tg::error!("invalid database outbox id length"))?;

	Ok(Id::new(value))
}

fn partition(value: u64) -> tg::Result<i64> {
	value
		.try_into()
		.map_err(|_| tg::error!("the database outbox partition exceeded an i64"))
}
