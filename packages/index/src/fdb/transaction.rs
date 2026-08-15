use {
	foundationdb as fdb,
	std::{
		future::Future,
		ops::{ControlFlow, Deref},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Copy)]
enum Mode {
	Read,
	Write,
}

#[derive(Clone)]
pub(crate) struct Transaction {
	inner: Arc<fdb::Transaction>,
}

pub(crate) async fn run<T, F, Fut>(database: &fdb::Database, operation: F) -> tg::Result<T>
where
	F: Fn(Transaction) -> Fut,
	Fut: Future<Output = tg::Result<ControlFlow<T, fdb::FdbError>>>,
{
	run_with_mode(database, Mode::Write, operation).await
}

pub(crate) async fn run_read<T, F, Fut>(database: &fdb::Database, operation: F) -> tg::Result<T>
where
	F: Fn(Transaction) -> Fut,
	Fut: Future<Output = tg::Result<ControlFlow<T, fdb::FdbError>>>,
{
	run_with_mode(database, Mode::Read, operation).await
}

impl Transaction {
	pub(super) fn new(transaction: fdb::Transaction) -> Self {
		let inner = Arc::new(transaction);

		Self { inner }
	}

	pub(super) fn take(self) -> tg::Result<fdb::Transaction> {
		Arc::try_unwrap(self.inner)
			.map_err(|_| tg::error!("a reference to the transaction was retained"))
	}
}

impl Deref for Transaction {
	type Target = fdb::Transaction;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

async fn run_with_mode<T, F, Fut>(
	database: &fdb::Database,
	mode: Mode,
	operation: F,
) -> tg::Result<T>
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

		match mode {
			Mode::Read => {
				drop(transaction.take()?);

				return Ok(value);
			},
			Mode::Write => {},
		}

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
