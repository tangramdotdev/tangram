macro_rules! run {
	($database:expr, |$transaction:ident| $body:expr $(,)?) => {{
		use {
			tangram_database::{Database as _, Error as _, Transaction as _},
			$crate::database::{Database, Transaction},
		};

		let options = match $database {
			#[cfg(feature = "turso")]
			Database::Turso(database) => database.retry(),
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => database.retry(),
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => database.retry(),
		};
		tangram_futures::retry::retry(&options, || async {
			match $database {
				#[cfg(feature = "turso")]
				Database::Turso(database) => {
					let mut connection = tangram_database::Database::write_connection(database)
						.await
						.map_err($crate::database::Error::Turso)?;
					let inner = tangram_database::Connection::transaction(&mut connection)
						.await
						.map_err($crate::database::Error::Turso)?;
					let transaction = Transaction::Turso(inner);
					let value = match async {
						let $transaction = &transaction;
						$body
					}
					.await
					{
						Ok(::std::ops::ControlFlow::Break(value)) => value,
						Ok(::std::ops::ControlFlow::Continue(error)) => {
							return Ok(::std::ops::ControlFlow::Continue(error));
						},
						Err(error) => return Err($crate::database::Error::from(error)),
					};
					if let Err(error) = transaction.commit().await {
						if $crate::database::Error::is_retry(&error) {
							return Ok(::std::ops::ControlFlow::Continue(error));
						}
						return Err(error);
					}
					Ok(::std::ops::ControlFlow::Break(value))
				},
				#[cfg(feature = "postgres")]
				Database::Postgres(database) => {
					let mut connection = tangram_database::Database::write_connection(database)
						.await
						.map_err($crate::database::Error::Postgres)?;
					let inner = tangram_database::Connection::transaction(&mut connection)
						.await
						.map_err($crate::database::Error::Postgres)?;
					let transaction = Transaction::Postgres(inner);
					let value = match async {
						let $transaction = &transaction;
						$body
					}
					.await
					{
						Ok(::std::ops::ControlFlow::Break(value)) => value,
						Ok(::std::ops::ControlFlow::Continue(error)) => {
							return Ok(::std::ops::ControlFlow::Continue(error));
						},
						Err(error) => return Err($crate::database::Error::from(error)),
					};
					if let Err(error) = transaction.commit().await {
						if $crate::database::Error::is_retry(&error) {
							return Ok(::std::ops::ControlFlow::Continue(error));
						}
						return Err(error);
					}
					Ok(::std::ops::ControlFlow::Break(value))
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(database) => {
					let mut connection = tangram_database::Database::write_connection(database)
						.await
						.map_err($crate::database::Error::Sqlite)?;
					let inner = tangram_database::Connection::transaction(&mut connection)
						.await
						.map_err($crate::database::Error::Sqlite)?;
					let transaction = Transaction::Sqlite(inner);
					let value = match async {
						let $transaction = &transaction;
						$body
					}
					.await
					{
						Ok(::std::ops::ControlFlow::Break(value)) => value,
						Ok(::std::ops::ControlFlow::Continue(error)) => {
							return Ok(::std::ops::ControlFlow::Continue(error));
						},
						Err(error) => return Err($crate::database::Error::from(error)),
					};
					if let Err(error) = transaction.commit().await {
						if $crate::database::Error::is_retry(&error) {
							return Ok(::std::ops::ControlFlow::Continue(error));
						}
						return Err(error);
					}
					Ok(::std::ops::ControlFlow::Break(value))
				},
			}
		})
		.await
		.map_err(|error| tangram_client::error!(!error, "database error"))
	}};
}

pub(crate) use run;
