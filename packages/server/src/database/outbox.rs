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
	pub async fn enqueue_database_outbox_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		arg: &tangram_index::batch::Arg,
	) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
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
		let config = self.config.database.outbox();
		let items = regions
			.into_iter()
			.map(|region| EnqueueItem {
				partition: rand::random_range(0..config.partition_total),
				region,
			})
			.collect();
		let id = Id::new(uuid::Uuid::now_v7().into_bytes());
		let payload = arg.serialize()?.into();
		let arg = EnqueueArg { id, items, payload };
		transaction.enqueue_outbox(arg).await
	}
}

impl Database {
	pub async fn delete_outbox(&self, arg: DeleteArg) -> tg::Result<()> {
		self.run(|transaction| {
			let arg = arg.clone();
			async move {
				transaction.delete_outbox(arg).await?;
				Ok::<_, super::Error>(ControlFlow::Break(()))
			}
			.boxed()
		})
		.await
	}

	pub async fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let partition_end = partition(arg.partition_end)?;
		let partition_start = partition(arg.partition_start)?;
		let batch_size = i64::try_from(arg.batch_size)
			.map_err(|_| tg::error!("the database outbox batch size exceeded an i64"))?;
		let connection = self
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r"
				select id, partition, payload
				from outbox
				where region = {p}1 and partition >= {p}2 and partition < {p}3
				order by partition, id
				limit {p}4;
			"
		);
		let rows = connection
			.query_all_into::<Row>(
				statement.into(),
				db::params![arg.region, partition_start, partition_end, batch_size],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to dequeue the database outbox"))?;
		let items = rows
			.into_iter()
			.map(Item::try_from)
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(items)
	}

	pub async fn try_get_outbox_id_at_or_before(&self, arg: TryGetIdArg) -> tg::Result<Option<Id>> {
		let partition_end = partition(arg.partition_end)?;
		let partition_start = partition(arg.partition_start)?;
		let connection = self
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let p = connection.p();
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
		let id = connection
			.query_optional_value_into::<Bytes>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the database outbox id"))?;
		let id = id.as_ref().map(decode_id).transpose()?;

		Ok(id)
	}
}

impl Transaction<'_> {
	pub async fn enqueue_outbox(&self, arg: EnqueueArg) -> tg::Result<()> {
		if arg.items.is_empty() {
			return Ok(());
		}
		let p = self.p();
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
		self.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to enqueue the database outbox"))?;

		Ok(())
	}

	async fn delete_outbox(&self, arg: DeleteArg) -> tg::Result<()> {
		if arg.keys.is_empty() {
			return Ok(());
		}
		let p = self.p();
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
		self.execute(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the database outbox items"))?;

		Ok(())
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
