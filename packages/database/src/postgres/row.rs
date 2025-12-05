use {
	super::Error,
	tokio_postgres::{self as postgres},
};

pub use tangram_database_macro::PostgresRowDeserialize as Deserialize;

pub trait Deserialize: Sized {
	fn deserialize(row: &postgres::Row) -> Result<Self, Error>;
}
