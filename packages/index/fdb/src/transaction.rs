use {
	foundationdb as fdb,
	futures::{Stream, StreamExt as _, TryStreamExt as _},
	std::{future::Future, ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
};

#[derive(Clone)]
pub(crate) struct Transaction {
	inner: Arc<fdb::Transaction>,
}

pub(crate) async fn run<T, F, Fut>(database: &fdb::Database, operation: F) -> tg::Result<T>
where
	F: Fn(Transaction) -> Fut,
	Fut: Future<Output = tg::Result<ControlFlow<T, fdb::FdbError>>>,
{
	let transaction = database
		.create_trx()
		.map_err(|error| tg::error!(!error, "failed to create a transaction"))?;
	let mut transaction = Transaction::new(transaction);
	loop {
		let result = operation(transaction.clone()).await?;
		let value = match result {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => {
				transaction = transaction
					.take()?
					.on_error(error)
					.await
					.map(Transaction::new)
					.map_err(|error| tg::error!(!error, "failed to retry a transaction"))?;
				continue;
			},
		};

		match transaction.take()?.commit().await {
			Ok(_) => return Ok(value),
			Err(error) => {
				transaction = error
					.on_error()
					.await
					.map(Transaction::new)
					.map_err(|error| tg::error!(!error, "failed to retry a transaction"))?;
			},
		}
	}
}

impl Transaction {
	#[must_use]
	pub(crate) fn new(transaction: fdb::Transaction) -> Self {
		let inner = Arc::new(transaction);

		Self { inner }
	}

	pub(crate) fn add_conflict_range(
		&self,
		begin: &[u8],
		end: &[u8],
		kind: fdb::options::ConflictRangeType,
	) -> fdb::FdbResult<()> {
		self.inner.add_conflict_range(begin, end, kind)
	}

	pub(crate) fn atomic_op(&self, key: &[u8], value: &[u8], kind: fdb::options::MutationType) {
		self.inner.atomic_op(key, value, kind);
	}

	pub(crate) fn clear(&self, key: &[u8]) {
		self.inner.clear(key);
	}

	pub(crate) fn clear_range(&self, begin: &[u8], end: &[u8]) {
		self.inner.clear_range(begin, end);
	}

	pub(crate) fn get<'a>(
		&'a self,
		key: &'a [u8],
		snapshot: bool,
	) -> impl Future<Output = fdb::FdbResult<Option<fdb::future::FdbSlice>>> + Send + 'a {
		self.inner.get(key, snapshot)
	}

	pub(crate) fn get_key<'a>(
		&'a self,
		selector: &'a fdb::KeySelector<'a>,
		snapshot: bool,
	) -> impl Future<Output = fdb::FdbResult<fdb::future::FdbSlice>> + Send + 'a {
		self.inner.get_key(selector, snapshot)
	}

	pub(crate) fn get_range<'a>(
		&'a self,
		option: &'a fdb::RangeOption<'a>,
		iteration: usize,
		snapshot: bool,
	) -> impl Future<Output = fdb::FdbResult<fdb::future::FdbValues>> + Send + 'a {
		self.inner.get_range(option, iteration, snapshot)
	}

	pub(crate) fn get_ranges_keyvalues<'a>(
		&'a self,
		option: fdb::RangeOption<'a>,
		snapshot: bool,
	) -> impl Stream<Item = fdb::FdbResult<fdb::future::FdbValue>> + Send + Unpin + 'a {
		futures::stream::unfold((1, Some(option)), move |(iteration, option)| async move {
			let option = option?;
			let result = self.get_range(&option, iteration, snapshot).await;
			let next = match &result {
				Ok(values) => option.next_range(values),
				Err(_) => None,
			};

			Some((result, (iteration + 1, next)))
		})
		.map_ok(|values| futures::stream::iter(values.into_iter().map(Ok)))
		.try_flatten()
		.boxed()
	}

	pub(crate) fn get_read_version(&self) -> impl Future<Output = fdb::FdbResult<i64>> + Send + '_ {
		self.inner.get_read_version()
	}

	pub(crate) fn set(&self, key: &[u8], value: &[u8]) {
		self.inner.set(key, value);
	}

	pub(crate) fn set_option(&self, option: fdb::options::TransactionOption) -> fdb::FdbResult<()> {
		self.inner.set_option(option)
	}

	pub(crate) fn take(self) -> tg::Result<fdb::Transaction> {
		Arc::try_unwrap(self.inner)
			.map_err(|_| tg::error!("a reference to the transaction was retained"))
	}
}
