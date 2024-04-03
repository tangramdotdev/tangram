use derive_more::{From, TryInto, TryUnwrap};

pub mod de;
pub mod ser;

#[derive(Clone, Debug, From, serde::Deserialize, serde::Serialize, TryInto, TryUnwrap)]
#[serde(untagged)]
#[try_unwrap(ref)]
pub enum Value {
	Null,
	Integer(i64),
	Real(f64),
	Text(String),
	Blob(Vec<u8>),
}
