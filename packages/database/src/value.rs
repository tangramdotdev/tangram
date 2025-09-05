use bytes::Bytes;

pub use self::{deserialize::Deserialize, json::Json, serde::Serde, serialize::Serialize};

pub mod deserialize;
pub mod json;
pub mod serde;
pub mod serialize;

#[derive(
	Clone,
	Debug,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	::serde::Deserialize,
	::serde::Serialize,
)]
#[serde(untagged)]
#[try_unwrap(ref)]
pub enum Value {
	Null,
	Integer(i64),
	Real(f64),
	Text(String),
	Blob(Bytes),
}
