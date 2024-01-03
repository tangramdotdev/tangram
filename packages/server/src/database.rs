use std::path::Path;
use tangram_error::{Result, WrapErr};

pub struct Database {
	pool: tangram_pool::Pool<rusqlite::Connection>,
}

impl Database {
	pub async fn new(path: &Path) -> Result<Self> {
		let pool = tangram_pool::Pool::new();
		let n = std::thread::available_parallelism().unwrap().get();
		for _ in 0..n {
			let db = rusqlite::Connection::open(path).wrap_err("Failed to open the database.")?;
			db.pragma_update(None, "busy_timeout", "100")
				.wrap_err("Failed to set the busy timeout.")?;
			pool.put(db).await;
		}
		Ok(Database { pool })
	}

	pub async fn get(&self) -> Result<tangram_pool::Guard<rusqlite::Connection>> {
		Ok(self.pool.get().await)
	}
}

pub struct Json<T>(pub T);

impl<T> rusqlite::types::ToSql for Json<T>
where
	T: serde::Serialize,
{
	fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
		let json = serde_json::to_string(&self.0)
			.map_err(|error| rusqlite::Error::ToSqlConversionFailure(error.into()))?;
		Ok(rusqlite::types::ToSqlOutput::Owned(
			rusqlite::types::Value::Text(json),
		))
	}
}

impl<T> rusqlite::types::FromSql for Json<T>
where
	T: serde::de::DeserializeOwned,
{
	fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
		let json = value.as_str()?;
		let value = serde_json::from_str(json)
			.map_err(|error| rusqlite::types::FromSqlError::Other(error.into()))?;
		Ok(Self(value))
	}
}

#[macro_export]
macro_rules! params {
	($($x:expr),* $(,)?) => {
		&[$(&$x as &(dyn rusqlite::types::ToSql),)*] as &[&(dyn rusqlite::types::ToSql)]
	};
}
