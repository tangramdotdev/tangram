use crate::Row;

pub use tangram_database_macro::RowDeserialize as Deserialize;

pub trait Deserialize: Sized {
	fn deserialize(row: Row) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>;
}
