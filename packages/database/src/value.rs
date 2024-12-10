pub use self::json::Json;

pub mod de;
pub mod json;
pub mod ser;

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	serde::Deserialize,
	serde::Serialize,
)]
#[serde(untagged)]
#[try_unwrap(ref)]
pub enum Value {
	Null,
	Integer(i64),
	Real(f64),
	Text(String),
	Blob(Vec<u8>),
}
