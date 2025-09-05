use num::ToPrimitive as _;
use serde::ser::Error as _;

pub struct Serializer<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
}

pub struct SerializeSeq<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	values: Vec<v8::Local<'s, v8::Value>>,
}

pub struct SerializeTuple<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	values: Vec<v8::Local<'s, v8::Value>>,
}

pub struct SerializeTupleStruct<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	values: Vec<v8::Local<'s, v8::Value>>,
}

pub struct SerializeTupleVariant<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	values: Vec<v8::Local<'s, v8::Value>>,
	variant: &'static str,
}

pub struct SerializeMap<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	key: Option<v8::Local<'s, v8::Value>>,
	object: v8::Local<'s, v8::Object>,
}

pub struct SerializeStruct<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	object: v8::Local<'s, v8::Object>,
}

pub struct SerializeStructVariant<'a, 's> {
	scope: &'a mut v8::HandleScope<'s>,
	object: v8::Local<'s, v8::Object>,
	variant: &'static str,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);

impl<'a, 's> Serializer<'a, 's> {
	pub fn new(scope: &'a mut v8::HandleScope<'s>) -> Self {
		Self { scope }
	}
}

impl<'a, 's> serde::Serializer for Serializer<'a, 's> {
	type Ok = v8::Local<'s, v8::Value>;
	type Error = Error;
	type SerializeSeq = SerializeSeq<'a, 's>;
	type SerializeTuple = SerializeTuple<'a, 's>;
	type SerializeTupleStruct = SerializeTupleStruct<'a, 's>;
	type SerializeTupleVariant = SerializeTupleVariant<'a, 's>;
	type SerializeMap = SerializeMap<'a, 's>;
	type SerializeStruct = SerializeStruct<'a, 's>;
	type SerializeStructVariant = SerializeStructVariant<'a, 's>;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		Ok(v8::Boolean::new(self.scope, v).into())
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v.into())
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		if let Some(v) = v.to_i32() {
			Ok(v8::Integer::new(self.scope, v).into())
		} else if let Some(v) = v.to_f64() {
			Ok(v8::Number::new(self.scope, v).into())
		} else {
			Ok(v8::BigInt::new_from_i64(self.scope, v).into())
		}
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v.into())
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v.into())
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v.into())
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		if let Some(v) = v.to_u32() {
			Ok(v8::Integer::new_from_unsigned(self.scope, v).into())
		} else if let Some(v) = v.to_f64() {
			Ok(v8::Number::new(self.scope, v).into())
		} else {
			Ok(v8::BigInt::new_from_u64(self.scope, v).into())
		}
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		self.serialize_f64(v.into())
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		Ok(v8::Number::new(self.scope, v).into())
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(&v.to_string())
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		Ok(v8::String::new(self.scope, v)
			.ok_or_else(|| Error::custom("failed to create the string"))?
			.into())
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		let bytes = v.to_owned();
		let len = v.len();
		let backing_store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
		let array_buffer = v8::ArrayBuffer::with_backing_store(self.scope, &backing_store);
		let uint8_array = v8::Uint8Array::new(self.scope, array_buffer, 0, len).unwrap();
		Ok(uint8_array.into())
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(v8::undefined(self.scope).into())
	}

	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Ok(v8::undefined(self.scope).into())
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
		self.serialize_unit()
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(variant)
	}

	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + serde::Serialize,
	{
		let object = v8::Object::new(self.scope);
		let key = v8::String::new_external_onebyte_static(self.scope, variant.as_bytes()).unwrap();
		let value = value.serialize(Serializer::new(self.scope))?;
		object.set(self.scope, key.into(), value).unwrap();
		Ok(object.into())
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Ok(SerializeSeq {
			scope: self.scope,
			values: Vec::new(),
		})
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeTuple {
			scope: self.scope,
			values: Vec::new(),
		})
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		Ok(SerializeTupleStruct {
			scope: self.scope,
			values: Vec::new(),
		})
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		Ok(SerializeTupleVariant {
			scope: self.scope,
			values: Vec::new(),
			variant,
		})
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		let object = v8::Object::new(self.scope);
		Ok(SerializeMap {
			scope: self.scope,
			key: None,
			object,
		})
	}

	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		let object = v8::Object::new(self.scope);
		Ok(SerializeStruct {
			scope: self.scope,
			object,
		})
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		let object = v8::Object::new(self.scope);
		Ok(SerializeStructVariant {
			scope: self.scope,
			object,
			variant,
		})
	}
}

impl<'s> serde::ser::SerializeSeq for SerializeSeq<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.scope))?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(v8::Array::new_with_elements(self.scope, &self.values).into())
	}
}

impl<'s> serde::ser::SerializeTuple for SerializeTuple<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.scope))?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(v8::Array::new_with_elements(self.scope, &self.values).into())
	}
}

impl<'s> serde::ser::SerializeTupleStruct for SerializeTupleStruct<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.scope))?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(v8::Array::new_with_elements(self.scope, &self.values).into())
	}
}

impl<'s> serde::ser::SerializeTupleVariant for SerializeTupleVariant<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.scope))?;
		self.values.push(value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let object = v8::Object::new(self.scope);
		let key =
			v8::String::new_external_onebyte_static(self.scope, self.variant.as_bytes()).unwrap();
		let value = v8::Array::new_with_elements(self.scope, &self.values);
		object.set(self.scope, key.into(), value.into()).unwrap();
		Ok(object.into())
	}
}

impl<'s> serde::ser::SerializeMap for SerializeMap<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = key.serialize(Serializer::new(self.scope))?;
		self.key.replace(key);
		Ok(())
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = self
			.key
			.take()
			.ok_or_else(|| Error::custom("missing key"))?;
		let value = value.serialize(Serializer::new(self.scope))?;
		self.object.set(self.scope, key, value).unwrap();
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.object.into())
	}
}

impl<'s> serde::ser::SerializeStruct for SerializeStruct<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = v8::String::new_external_onebyte_static(self.scope, key.as_bytes()).unwrap();
		let value = value.serialize(Serializer::new(self.scope))?;
		self.object.set(self.scope, key.into(), value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.object.into())
	}
}

impl<'s> serde::ser::SerializeStructVariant for SerializeStructVariant<'_, 's> {
	type Ok = v8::Local<'s, v8::Value>;

	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = v8::String::new_external_onebyte_static(self.scope, key.as_bytes()).unwrap();
		let value = value.serialize(Serializer::new(self.scope))?;
		self.object.set(self.scope, key.into(), value);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let object = v8::Object::new(self.scope);
		let key =
			v8::String::new_external_onebyte_static(self.scope, self.variant.as_bytes()).unwrap();
		let value = self.object;
		object.set(self.scope, key.into(), value.into()).unwrap();
		Ok(object.into())
	}
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: std::fmt::Display,
	{
		Self(msg.to_string().into())
	}
}
