#[macro_export]
macro_rules! run {
	($database:expr, |$transaction:ident| $body:expr $(,)?) => {{
		use $crate::prelude::*;

		let options = $database.retry();
		tangram_futures::retry::retry(&options, || async {
			let mut connection = $database.write_connection().await?;
			let transaction = connection.transaction().await?;
			let value = match async {
				let $transaction = &transaction;
				$body
			}
			.await
			{
				Ok(::std::ops::ControlFlow::Break(value)) => value,
				Ok(::std::ops::ControlFlow::Continue(error)) => {
					transaction.rollback().await.ok();
					return Ok(::std::ops::ControlFlow::Continue(error));
				},
				Err(error) => return Err($crate::Error::other(error)),
			};
			if let Err(error) = transaction.commit().await {
				if $crate::Error::is_retry(&error) {
					return Ok(::std::ops::ControlFlow::Continue(error));
				}
				return Err(error);
			}
			Ok(::std::ops::ControlFlow::Break(value))
		})
		.await
	}};
}
