use {
	foundationdb as fdb,
	futures::{Stream, StreamExt as _, TryStreamExt as _},
	std::{future::Future, ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tokio::sync::Semaphore,
};

#[derive(Clone)]
pub(crate) struct Transaction {
	inner: Arc<fdb::Transaction>,
	read_semaphore: Option<Arc<Semaphore>>,
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

		Self {
			inner,
			read_semaphore: None,
		}
	}

	#[must_use]
	pub(crate) fn with_read_semaphore(&self, read_semaphore: Arc<Semaphore>) -> Self {
		Self {
			inner: self.inner.clone(),
			read_semaphore: Some(read_semaphore),
		}
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
		limit_read(self.read_semaphore.as_deref(), move || {
			self.inner.get(key, snapshot)
		})
	}

	pub(crate) fn get_key<'a>(
		&'a self,
		selector: &'a fdb::KeySelector<'a>,
		snapshot: bool,
	) -> impl Future<Output = fdb::FdbResult<fdb::future::FdbSlice>> + Send + 'a {
		limit_read(self.read_semaphore.as_deref(), move || {
			self.inner.get_key(selector, snapshot)
		})
	}

	pub(crate) fn get_range<'a>(
		&'a self,
		option: &'a fdb::RangeOption<'a>,
		iteration: usize,
		snapshot: bool,
	) -> impl Future<Output = fdb::FdbResult<fdb::future::FdbValues>> + Send + 'a {
		limit_read(self.read_semaphore.as_deref(), move || {
			self.inner.get_range(option, iteration, snapshot)
		})
	}

	pub(crate) fn get_ranges_keyvalues<'a>(
		&'a self,
		option: fdb::RangeOption<'a>,
		snapshot: bool,
	) -> impl Stream<Item = fdb::FdbResult<fdb::future::FdbValue>> + Send + Unpin + 'a {
		// Limit each range page independently when the transaction has a read semaphore.
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
		limit_read(self.read_semaphore.as_deref(), || {
			self.inner.get_read_version()
		})
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

fn limit_read<'a, T, F, Fut>(
	semaphore: Option<&'a Semaphore>,
	operation: F,
) -> impl Future<Output = T> + Send + 'a
where
	F: FnOnce() -> Fut + Send + 'a,
	Fut: Future<Output = T> + Send + 'a,
	T: Send + 'a,
{
	async move {
		let permit = match semaphore {
			Some(semaphore) => Some(semaphore.acquire().await.unwrap()),
			None => None,
		};
		// Create the FoundationDB future only after acquiring any required permit.
		let output = operation().await;
		drop(permit);

		output
	}
}

#[cfg(test)]
mod tests {
	use {super::*, std::sync::Mutex};

	#[tokio::test]
	async fn limits_reads() {
		let counts = Arc::new(Mutex::new((0, 0)));
		let semaphore = Arc::new(Semaphore::new(2));
		let futures = (0..8).map(|_| {
			let counts = counts.clone();
			let semaphore = semaphore.clone();
			async move {
				limit_read(Some(&semaphore), || async {
					{
						let mut counts = counts.lock().unwrap();
						counts.0 += 1;
						counts.1 = counts.1.max(counts.0);
					}
					tokio::task::yield_now().await;
					counts.lock().unwrap().0 -= 1;
				})
				.await;
			}
		});

		futures::future::join_all(futures).await;

		assert_eq!(counts.lock().unwrap().1, 2);
	}

	#[tokio::test]
	async fn releases_a_permit_when_cancelled() {
		let semaphore = Semaphore::new(1);
		let (sender, mut receiver) = tokio::sync::oneshot::channel();
		let mut read = Box::pin(limit_read(Some(&semaphore), || async move {
			sender.send(()).unwrap();
			std::future::pending::<()>().await;
		}));
		tokio::select! {
			() = &mut read => unreachable!(),
			result = &mut receiver => result.unwrap(),
		}
		assert_eq!(semaphore.available_permits(), 0);
		drop(read);

		assert_eq!(semaphore.available_permits(), 1);
	}
}
