use {num::ToPrimitive as _, rquickjs as qjs, serde::ser::Error as _};

pub struct Serializer<'js> {
	ctx: qjs::Ctx<'js>,
}

pub struct SerializeSeq<'js> {
	ctx: qjs::Ctx<'js>,
	array: qjs::Array<'js>,
}

pub struct SerializeTuple<'js> {
	ctx: qjs::Ctx<'js>,
	array: qjs::Array<'js>,
}

pub struct SerializeTupleStruct<'js> {
	ctx: qjs::Ctx<'js>,
	array: qjs::Array<'js>,
}

pub struct SerializeTupleVariant<'js> {
	ctx: qjs::Ctx<'js>,
	array: qjs::Array<'js>,
	variant: &'static str,
}

pub struct SerializeMap<'js> {
	ctx: qjs::Ctx<'js>,
	key: Option<qjs::Value<'js>>,
	object: qjs::Object<'js>,
}

pub struct SerializeStruct<'js> {
	ctx: qjs::Ctx<'js>,
	object: qjs::Object<'js>,
}

pub struct SerializeStructVariant<'js> {
	ctx: qjs::Ctx<'js>,
	object: qjs::Object<'js>,
	variant: &'static str,
}

#[derive(Debug)]
pub struct Error(Box<dyn std::error::Error + Send + Sync + 'static>);

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.0.source()
	}
}

impl<'js> Serializer<'js> {
	pub fn new(ctx: qjs::Ctx<'js>) -> Self {
		Self { ctx }
	}
}

impl<'js> serde::Serializer for Serializer<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;
	type SerializeSeq = SerializeSeq<'js>;
	type SerializeTuple = SerializeTuple<'js>;
	type SerializeTupleStruct = SerializeTupleStruct<'js>;
	type SerializeTupleVariant = SerializeTupleVariant<'js>;
	type SerializeMap = SerializeMap<'js>;
	type SerializeStruct = SerializeStruct<'js>;
	type SerializeStructVariant = SerializeStructVariant<'js>;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		Ok(qjs::Value::new_bool(self.ctx, v))
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
			Ok(qjs::Value::new_int(self.ctx, v))
		} else if let Some(v) = v.to_f64() {
			Ok(qjs::Value::new_float(self.ctx, v))
		} else {
			Ok(qjs::Value::new_float(self.ctx, v.to_f64().unwrap()))
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
		if let Some(v) = v.to_i32() {
			Ok(qjs::Value::new_int(self.ctx, v))
		} else if let Some(v) = v.to_f64() {
			Ok(qjs::Value::new_float(self.ctx, v))
		} else {
			Ok(qjs::Value::new_float(self.ctx, v.to_f64().unwrap()))
		}
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		self.serialize_f64(v.into())
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		Ok(qjs::Value::new_float(self.ctx, v))
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(&v.to_string())
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		let string =
			qjs::String::from_str(self.ctx, v).map_err(|error| Error::custom(error.to_string()))?;
		Ok(string.into_value())
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		let typed_array = qjs::TypedArray::<u8>::new(self.ctx, v)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(typed_array.into_value())
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(qjs::Value::new_undefined(self.ctx))
	}

	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Ok(qjs::Value::new_undefined(self.ctx))
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
		let object =
			qjs::Object::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		let inner_value = value.serialize(Serializer::new(self.ctx))?;
		object
			.set(variant, inner_value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(object.into_value())
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		let array =
			qjs::Array::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeSeq {
			ctx: self.ctx,
			array,
		})
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		let array =
			qjs::Array::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeTuple {
			ctx: self.ctx,
			array,
		})
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		let array =
			qjs::Array::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeTupleStruct {
			ctx: self.ctx,
			array,
		})
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let array =
			qjs::Array::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeTupleVariant {
			ctx: self.ctx,
			array,
			variant,
		})
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		let object =
			qjs::Object::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeMap {
			ctx: self.ctx,
			key: None,
			object,
		})
	}

	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		let object =
			qjs::Object::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeStruct {
			ctx: self.ctx,
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
		let object =
			qjs::Object::new(self.ctx.clone()).map_err(|error| Error::custom(error.to_string()))?;
		Ok(SerializeStructVariant {
			ctx: self.ctx,
			object,
			variant,
		})
	}
}

impl<'js> serde::ser::SerializeSeq for SerializeSeq<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		let index = self.array.len();
		self.array
			.set(index, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.array.into_value())
	}
}

impl<'js> serde::ser::SerializeTuple for SerializeTuple<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		let index = self.array.len();
		self.array
			.set(index, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.array.into_value())
	}
}

impl<'js> serde::ser::SerializeTupleStruct for SerializeTupleStruct<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		let index = self.array.len();
		self.array
			.set(index, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.array.into_value())
	}
}

impl<'js> serde::ser::SerializeTupleVariant for SerializeTupleVariant<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		let index = self.array.len();
		self.array
			.set(index, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let object =
			qjs::Object::new(self.ctx).map_err(|error| Error::custom(error.to_string()))?;
		object
			.set(self.variant, self.array)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(object.into_value())
	}
}

impl<'js> serde::ser::SerializeMap for SerializeMap<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let key = key.serialize(Serializer::new(self.ctx.clone()))?;
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
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		self.object
			.set(key, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.object.into_value())
	}
}

impl<'js> serde::ser::SerializeStruct for SerializeStruct<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		self.object
			.set(key, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.object.into_value())
	}
}

impl<'js> serde::ser::SerializeStructVariant for SerializeStructVariant<'js> {
	type Ok = qjs::Value<'js>;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize + ?Sized,
	{
		let value = value.serialize(Serializer::new(self.ctx.clone()))?;
		self.object
			.set(key, value)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let object =
			qjs::Object::new(self.ctx).map_err(|error| Error::custom(error.to_string()))?;
		object
			.set(self.variant, self.object)
			.map_err(|error| Error::custom(error.to_string()))?;
		Ok(object.into_value())
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
