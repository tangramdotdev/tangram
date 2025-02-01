pub use self::serde::Serde;
use bytes::Bytes;
use num::ToPrimitive as _;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tangram_client as tg;
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

mod artifact;
mod blob;
mod branch;
mod checksum;
mod command;
mod de;
mod directory;
mod error;
mod file;
mod graph;
mod leaf;
mod module;
mod mutation;
mod object;
mod process;
mod reference;
mod referent;
mod ser;
mod serde;
mod symlink;
mod template;
mod value;

pub trait ToV8 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>>;
}

pub trait FromV8: Sized {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self>;
}

impl ToV8 for () {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::undefined(scope).into())
	}
}

impl<T> ToV8 for &T
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		(*self).to_v8(scope)
	}
}

impl FromV8 for () {
	fn from_v8<'a>(
		_scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_null_or_undefined()
			&& !v8::Local::<v8::Array>::try_from(value).is_ok_and(|array| array.length() == 0)
		{
			return Err(tg::error!("expected null, undefined, or an empty array"));
		}
		Ok(())
	}
}

impl ToV8 for bool {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Boolean::new(scope, *self).into())
	}
}

impl FromV8 for bool {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Boolean>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a boolean value"))?;
		let value = value.boolean_value(scope);
		Ok(value)
	}
}

impl ToV8 for u8 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for u8 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_u8()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for u16 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for u16 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_u16()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for u32 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for u32 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_u32()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for u64 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for u64 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_u64()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for usize {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for usize {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_usize()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for i8 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for i8 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_i8()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for i16 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for i16 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_i16()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for i32 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for i32 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_i32()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for i64 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for i64 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_i64()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for isize {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for isize {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_isize()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for f32 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(
			scope,
			self.to_f64().ok_or_else(|| tg::error!("invalid number"))?,
		)
		.into())
	}
}

impl FromV8 for f32 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))?
			.to_f32()
			.ok_or_else(|| tg::error!("invalid number"))
	}
}

impl ToV8 for f64 {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::Number::new(scope, *self).into())
	}
}

impl FromV8 for f64 {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))
	}
}

impl<T> ToV8 for Arc<T>
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.as_ref().to_v8(scope)
	}
}

impl<T> FromV8 for Arc<T>
where
	T: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		Ok(Self::new(<_>::from_v8(scope, value)?))
	}
}

impl<L, R> ToV8 for Either<L, R>
where
	L: ToV8,
	R: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Either::Left(s) => s.to_v8(scope),
			Either::Right(s) => s.to_v8(scope),
		}
	}
}

impl<L, R> FromV8 for Either<L, R>
where
	L: FromV8,
	R: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		<_>::from_v8(scope, value)
			.map(Either::Left)
			.or_else(|_| <_>::from_v8(scope, value).map(Either::Right))
	}
}

impl<T> ToV8 for Option<T>
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		match self {
			Some(value) => value.to_v8(scope),
			None => Ok(v8::undefined(scope).into()),
		}
	}
}

impl<T> FromV8 for Option<T>
where
	T: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if value.is_null_or_undefined() {
			Ok(None)
		} else {
			Ok(Some(<_>::from_v8(scope, value)?))
		}
	}
}

impl<T1> ToV8 for (T1,)
where
	T1: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value = self.0.to_v8(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value]);
		Ok(value.into())
	}
}

impl<T1> FromV8 for (T1,)
where
	T1: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let value0 = value
			.get_index(scope, 0)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value0 = <_>::from_v8(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		Ok((value0,))
	}
}

impl<T1, T2> ToV8 for (T1, T2)
where
	T1: ToV8,
	T2: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.to_v8(scope)?;
		let value1 = self.1.to_v8(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1]);
		Ok(value.into())
	}
}

impl<T1, T2> FromV8 for (T1, T2)
where
	T1: FromV8,
	T2: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let value0 = value
			.get_index(scope, 0)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value1 = value
			.get_index(scope, 1)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value0 = <_>::from_v8(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::from_v8(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		Ok((value0, value1))
	}
}

impl<T1, T2, T3> ToV8 for (T1, T2, T3)
where
	T1: ToV8,
	T2: ToV8,
	T3: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.to_v8(scope)?;
		let value1 = self.1.to_v8(scope)?;
		let value2 = self.2.to_v8(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1, value2]);
		Ok(value.into())
	}
}

impl<T1, T2, T3> FromV8 for (T1, T2, T3)
where
	T1: FromV8,
	T2: FromV8,
	T3: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let value0 = value
			.get_index(scope, 0)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value1 = value
			.get_index(scope, 1)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value2 = value
			.get_index(scope, 2)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value0 = <_>::from_v8(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::from_v8(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		let value2 = <_>::from_v8(scope, value2)
			.map_err(|source| tg::error!(!source, "failed to deserialize the third value"))?;
		Ok((value0, value1, value2))
	}
}

impl<T1, T2, T3, T4> ToV8 for (T1, T2, T3, T4)
where
	T1: ToV8,
	T2: ToV8,
	T3: ToV8,
	T4: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let value0 = self.0.to_v8(scope)?;
		let value1 = self.1.to_v8(scope)?;
		let value2 = self.2.to_v8(scope)?;
		let value3 = self.3.to_v8(scope)?;
		let value = v8::Array::new_with_elements(scope, &[value0, value1, value2, value3]);
		Ok(value.into())
	}
}

impl<T1, T2, T3, T4> FromV8 for (T1, T2, T3, T4)
where
	T1: FromV8,
	T2: FromV8,
	T3: FromV8,
	T4: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let value0 = value
			.get_index(scope, 0)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value1 = value
			.get_index(scope, 1)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value2 = value
			.get_index(scope, 2)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value3 = value
			.get_index(scope, 3)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value0 = <_>::from_v8(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::from_v8(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		let value2 = <_>::from_v8(scope, value2)
			.map_err(|source| tg::error!(!source, "failed to deserialize the third value"))?;
		let value3 = <_>::from_v8(scope, value3)
			.map_err(|source| tg::error!(!source, "failed to deserialize the fourth value"))?;
		Ok((value0, value1, value2, value3))
	}
}

impl<T> ToV8 for &[T]
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let values = self
			.iter()
			.map(|value| value.to_v8(scope))
			.collect::<tg::Result<Vec<_>>>()?;
		let value = v8::Array::new_with_elements(scope, &values);
		Ok(value.into())
	}
}

impl<T> ToV8 for Vec<T>
where
	T: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.as_slice().to_v8(scope)
	}
}

impl<T> FromV8 for Vec<T>
where
	T: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let len = value.length().to_usize().unwrap();
		let mut output = Vec::with_capacity(len);
		for i in 0..len {
			let value = value
				.get_index(scope, i.to_u32().unwrap())
				.ok_or_else(|| tg::error!("expected a value"))?;
			let value = <_>::from_v8(scope, value)
				.map_err(|source| tg::error!(!source, "failed to deserialize the value"))?;
			output.push(value);
		}
		Ok(output)
	}
}

impl<K, V> ToV8 for BTreeMap<K, V>
where
	K: ToV8,
	V: ToV8,
{
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let output = v8::Object::new(scope);
		for (key, value) in self {
			let key = key.to_v8(scope)?;
			let value = value.to_v8(scope)?;
			output.set(scope, key, value).unwrap();
		}
		Ok(output.into())
	}
}

impl<K, V> FromV8 for BTreeMap<K, V>
where
	K: FromV8 + Ord,
	V: FromV8,
{
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Object>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an object"))?;
		let args = v8::GetPropertyNamesArgsBuilder::new()
			.key_conversion(v8::KeyConversionMode::ConvertToString)
			.build();
		let property_names = value.get_own_property_names(scope, args).unwrap();
		let mut output = BTreeMap::new();
		for i in 0..property_names.length() {
			let key = property_names.get_index(scope, i).unwrap();
			let value = value.get(scope, key).unwrap();
			let key = <_>::from_v8(scope, key)
				.map_err(|source| tg::error!(!source, "failed to deserialize the key"))?;
			let value = <_>::from_v8(scope, value)
				.map_err(|source| tg::error!(!source, "failed to deserialize the value"))?;
			output.insert(key, value);
		}
		Ok(output)
	}
}

impl ToV8 for String {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		Ok(v8::String::new(scope, self)
			.ok_or_else(|| tg::error!("failed to create the string"))?
			.into())
	}
}

impl FromV8 for String {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_string() {
			return Err(tg::error!("expected a string"));
		}
		Ok(value.to_rust_string_lossy(scope))
	}
}

impl ToV8 for PathBuf {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let string = self
			.to_str()
			.ok_or_else(|| tg::error!("path is not a string"))?;
		let string = v8::String::new(scope, string)
			.ok_or_else(|| tg::error!("failed to create the string"))?;
		Ok(string.into())
	}
}

impl FromV8 for PathBuf {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_string() {
			return Err(tg::error!("expected a string"));
		}
		let string = value.to_rust_string_lossy(scope);
		let path = Self::from(string);
		Ok(path)
	}
}

impl ToV8 for Bytes {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		let bytes = self.to_vec();
		let len = bytes.len();
		let backing_store = v8::ArrayBuffer::new_backing_store_from_vec(bytes).make_shared();
		let array_buffer = v8::ArrayBuffer::with_backing_store(scope, &backing_store);
		let uint8_array = v8::Uint8Array::new(scope, array_buffer, 0, len).unwrap();
		Ok(uint8_array.into())
	}
}

impl FromV8 for Bytes {
	fn from_v8<'a>(
		_scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let uint8_array = v8::Local::<v8::Uint8Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a Uint8Array"))?;
		let bytes = if let Some(data) = uint8_array
			.get_backing_store()
			.and_then(|backing_store| backing_store.data())
		{
			let offset = uint8_array.byte_offset();
			let length = uint8_array.byte_length();
			let slice = unsafe {
				std::slice::from_raw_parts(data.cast::<u8>().as_ptr().add(offset), length)
			};
			Bytes::copy_from_slice(slice)
		} else {
			let length = uint8_array.byte_length();
			if length > 0 {
				return Err(tg::error!("invalid uint8array"));
			}
			Bytes::new()
		};
		Ok(bytes)
	}
}

impl ToV8 for time::OffsetDateTime {
	fn to_v8<'a>(&self, scope: &mut v8::HandleScope<'a>) -> tg::Result<v8::Local<'a, v8::Value>> {
		self.format(&Rfc3339)
			.map_err(|err| tg::error!(!err, "failed to format timestamp"))?
			.to_v8(scope)
	}
}

impl FromV8 for time::OffsetDateTime {
	fn from_v8<'a>(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let string = String::from_v8(scope, value)?;
		Self::parse(&string, &Rfc3339).map_err(|err| tg::error!(!err, "invalid RFC3339 timestamp"))
	}
}
