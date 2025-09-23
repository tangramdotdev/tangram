use {
	bytes::Bytes,
	num::ToPrimitive as _,
	std::{collections::BTreeMap, path::PathBuf, sync::Arc},
	tangram_client as tg,
	tangram_either::Either,
};

pub trait Serialize {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>>;
}

impl<T> Serialize for &T
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		(*self).serialize(scope)
	}
}

impl Serialize for () {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::undefined(scope).into())
	}
}

impl Serialize for bool {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Boolean::new(scope, *self).into())
	}
}

impl Serialize for u8 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for u16 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for u32 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for u64 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for usize {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for i8 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for i16 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for i32 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for i64 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for isize {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for f32 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl Serialize for f64 {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(scope, *self).into())
	}
}

impl<T> Serialize for Box<T>
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.as_ref().serialize(scope)
	}
}

impl<T> Serialize for Arc<T>
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.as_ref().serialize(scope)
	}
}

impl<L, R> Serialize for Either<L, R>
where
	L: Serialize,
	R: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Either::Left(s) => s.serialize(scope),
			Either::Right(s) => s.serialize(scope),
		}
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Some(value) => value.serialize(scope),
			None => Ok(v8::undefined(scope).into()),
		}
	}
}

impl<T1> Serialize for (T1,)
where
	T1: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = self.0.serialize(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value]);
		Ok(value.into())
	}
}

impl<T1, T2> Serialize for (T1, T2)
where
	T1: Serialize,
	T2: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.serialize(scope)?;
		let value1 = self.1.serialize(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1]);
		Ok(value.into())
	}
}

impl<T1, T2, T3> Serialize for (T1, T2, T3)
where
	T1: Serialize,
	T2: Serialize,
	T3: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.serialize(scope)?;
		let value1 = self.1.serialize(scope)?;
		let value2 = self.2.serialize(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1, value2]);
		Ok(value.into())
	}
}

impl<T1, T2, T3, T4> Serialize for (T1, T2, T3, T4)
where
	T1: Serialize,
	T2: Serialize,
	T3: Serialize,
	T4: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.serialize(scope)?;
		let value1 = self.1.serialize(scope)?;
		let value2 = self.2.serialize(scope)?;
		let value3 = self.3.serialize(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1, value2, value3]);
		Ok(value.into())
	}
}

impl<T> Serialize for &[T]
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let values = self
			.iter()
			.map(|value| value.serialize(scope))
			.collect::<tg::Result<Vec<_>>>()?;
		let value = v8::Array::new_with_elements(scope, &values);
		Ok(value.into())
	}
}

impl<T> Serialize for Vec<T>
where
	T: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.as_slice().serialize(scope)
	}
}

impl<K, V> Serialize for BTreeMap<K, V>
where
	K: Serialize,
	V: Serialize,
{
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let output = v8::Object::new(scope);
		for (key, value) in self {
			let key = key.serialize(scope)?;
			let value = value.serialize(scope)?;
			output.set(scope, key, value).unwrap();
		}
		Ok(output.into())
	}
}

impl Serialize for String {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::String::new(scope, self)
			.ok_or_else(|| tg::error!("failed to create the string"))?
			.into())
	}
}

impl Serialize for PathBuf {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let string = self
			.to_str()
			.ok_or_else(|| tg::error!("path is not a string"))?;
		let string = v8::String::new(scope, string)
			.ok_or_else(|| tg::error!("failed to create the string"))?;
		Ok(string.into())
	}
}

impl Serialize for Bytes {
	fn serialize<'a>(
		&self,
		scope: &mut v8::HandleScope<'a>,
	) -> tg::Result<v8::Local<'a, v8::Value>> {
		let bytes = self.to_vec();
		let len = bytes.len();
		let backing_store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
		let array_buffer = v8::ArrayBuffer::with_backing_store(scope, &backing_store);
		let uint8_array = v8::Uint8Array::new(scope, array_buffer, 0, len).unwrap();
		Ok(uint8_array.into())
	}
}
