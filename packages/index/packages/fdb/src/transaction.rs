use {foundationdb as fdb, std::ops::ControlFlow, tangram_client::prelude::*};

pub(crate) async fn run<T, F>(database: &fdb::Database, operation: F) -> tg::Result<T>
where
	F: AsyncFn(&fdb::Transaction) -> tg::Result<ControlFlow<T, fdb::FdbError>>,
{
	let mut transaction = database
		.create_trx()
		.map_err(|error| tg::error!(!error, "failed to create a transaction"))?;
	loop {
		let result = operation(&transaction).await?;
		let value = match result {
			ControlFlow::Break(value) => value,
			ControlFlow::Continue(error) => {
				transaction = transaction
					.on_error(error)
					.await
					.map_err(|error| tg::error!(!error, "failed to retry a transaction"))?;
				continue;
			},
		};

		match transaction.commit().await {
			Ok(_) => return Ok(value),
			Err(error) => {
				transaction = error
					.on_error()
					.await
					.map_err(|error| tg::error!(!error, "failed to retry a transaction"))?;
			},
		}
	}
}
