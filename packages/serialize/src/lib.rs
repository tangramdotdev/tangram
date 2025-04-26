pub use self::{deserializer::Deserializer, kind::Kind, serializer::Serializer, value::Value};
use std::io::{Read, Result, Write};
pub use tangram_serialize_macro::{Deserialize, Serialize};

#[cfg(feature = "bytes")]
mod bytes;
pub mod deserializer;
pub mod kind;
pub mod serializer;
pub mod types;
#[cfg(feature = "url")]
mod url;
pub mod value;

pub trait Deserialize: Sized {
	fn deserialize<R>(deserializer: &mut Deserializer<R>) -> Result<Self>
	where
		R: Read;
}

pub trait Serialize {
	fn serialize<W>(&self, serializer: &mut Serializer<W>) -> Result<()>
	where
		W: Write;
}

pub fn from_reader<T, R>(reader: R) -> Result<T>
where
	T: Deserialize,
	R: Read,
{
	let mut deserializer = Deserializer::new(reader);
	deserializer.deserialize()
}

pub fn to_writer<T, W>(value: &T, writer: &mut W) -> Result<()>
where
	T: Serialize,
	W: Write,
{
	let mut serializer = Serializer::new(writer);
	serializer.serialize(value)?;
	Ok(())
}

pub fn from_slice<T>(slice: &[u8]) -> Result<T>
where
	T: Deserialize,
{
	from_reader(slice)
}

pub fn to_vec<T>(value: &T) -> Result<Vec<u8>>
where
	T: Serialize,
{
	let mut bytes = Vec::new();
	to_writer(value, &mut bytes)?;
	Ok(bytes)
}

#[cfg(test)]
mod test {
	use crate as tangram_serialize;

	#[derive(Debug, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
	struct AddressBook {
		#[tangram_serialize(id = 0)]
		pub contacts: Vec<Contact>,
	}

	#[derive(Debug, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
	struct Contact {
		#[tangram_serialize(id = 0)]
		pub name: String,
		#[tangram_serialize(id = 1)]
		pub age: Option<u16>,
		#[tangram_serialize(id = 2)]
		pub phone_numbers: Vec<PhoneNumber>,
	}

	#[derive(Debug, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
	enum PhoneNumber {
		#[tangram_serialize(id = 0)]
		Home(String),
		#[tangram_serialize(id = 1)]
		Mobile(String),
	}

	#[test]
	fn test_address_book() {
		let right = AddressBook {
			contacts: vec![Contact {
				name: "John Doe".to_owned(),
				age: Some(21),
				phone_numbers: vec![PhoneNumber::Mobile("555-555-5555".to_owned())],
			}],
		};
		let bytes = tangram_serialize::to_vec(&right).unwrap();
		let left: AddressBook = tangram_serialize::from_slice(&bytes).unwrap();
		assert_eq!(left, right);
	}
}
