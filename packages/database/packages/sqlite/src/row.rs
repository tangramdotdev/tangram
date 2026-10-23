use {super::Error, rusqlite as sqlite};

pub use tangram_database_macro::SqliteRowDeserialize as Deserialize;

pub trait Deserialize: Sized {
	fn deserialize(row: &sqlite::Row) -> Result<Self, Error>;
}
