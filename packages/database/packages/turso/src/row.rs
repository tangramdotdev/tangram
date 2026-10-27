use super::Error;

pub use tangram_database_macro::TursoRowDeserialize as Deserialize;

pub trait Deserialize: Sized {
	fn deserialize(row: &turso::Row, columns: &[String]) -> Result<Self, Error>;
}
