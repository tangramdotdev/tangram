use {
	foundationdb as fdb,
	std::{
		future::Future,
		ops::{ControlFlow, Deref},
		sync::Arc,
	},
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
	fn new(transaction: fdb::Transaction) -> Self {
		let inner = Arc::new(transaction);

		Self { inner }
	}

	fn take(self) -> tg::Result<fdb::Transaction> {
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
