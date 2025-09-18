use bytes::Bytes;
use num::ToPrimitive as _;
use std::{collections::BTreeMap, path::PathBuf, sync::Arc};
use tangram_client as tg;
use tangram_either::Either;

pub trait Deserialize<'a>: 'a + Sized {
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self>;
}

impl<'a> Deserialize<'a> for v8::Local<'a, v8::Value> {
	fn deserialize(
		_scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tangram_client::Result<Self> {
		Ok(value)
	}
}

impl<'a> Deserialize<'a> for () {
	fn deserialize(
		_scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_null_or_undefined() {
			return Err(tg::error!("expected null or undefined"));
		}
		Ok(())
	}
}

impl<'a> Deserialize<'a> for bool {
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Boolean>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a boolean value"))?;
		let value = value.boolean_value(scope);
		Ok(value)
	}
}

impl<'a> Deserialize<'a> for u8 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for u16 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for u32 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for u64 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for usize {
	fn deserialize(
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

impl<'a> Deserialize<'a> for i8 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for i16 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for i32 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for i64 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for isize {
	fn deserialize(
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

impl<'a> Deserialize<'a> for f32 {
	fn deserialize(
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

impl<'a> Deserialize<'a> for f64 {
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		v8::Local::<v8::Number>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected a number"))?
			.number_value(scope)
			.ok_or_else(|| tg::error!("expected a number"))
	}
}

impl<'a, T> Deserialize<'a> for Box<T>
where
	T: Deserialize<'a>,
{
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		Ok(Self::new(<_>::deserialize(scope, value)?))
	}
}

impl<'a, T> Deserialize<'a> for Arc<T>
where
	T: Deserialize<'a>,
{
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		Ok(Self::new(<_>::deserialize(scope, value)?))
	}
}

impl<'a, L, R> Deserialize<'a> for Either<L, R>
where
	L: Deserialize<'a>,
	R: Deserialize<'a>,
{
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		<_>::deserialize(scope, value)
			.map(Either::Left)
			.or_else(|_| <_>::deserialize(scope, value).map(Either::Right))
	}
}

impl<'a, T> Deserialize<'a> for Option<T>
where
	T: Deserialize<'a>,
{
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if value.is_null_or_undefined() {
			Ok(None)
		} else {
			Ok(Some(<_>::deserialize(scope, value)?))
		}
	}
}

impl<'a, T1> Deserialize<'a> for (T1,)
where
	T1: Deserialize<'a>,
{
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		let value = v8::Local::<v8::Array>::try_from(value)
			.map_err(|source| tg::error!(!source, "expected an array"))?;
		let value0 = value
			.get_index(scope, 0)
			.ok_or_else(|| tg::error!("expected a value"))?;
		let value0 = <_>::deserialize(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		Ok((value0,))
	}
}

impl<'a, T1, T2> Deserialize<'a> for (T1, T2)
where
	T1: Deserialize<'a>,
	T2: Deserialize<'a>,
{
	fn deserialize(
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
		let value0 = <_>::deserialize(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::deserialize(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		Ok((value0, value1))
	}
}

impl<'a, T1, T2, T3> Deserialize<'a> for (T1, T2, T3)
where
	T1: Deserialize<'a>,
	T2: Deserialize<'a>,
	T3: Deserialize<'a>,
{
	fn deserialize(
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
		let value0 = <_>::deserialize(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::deserialize(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		let value2 = <_>::deserialize(scope, value2)
			.map_err(|source| tg::error!(!source, "failed to deserialize the third value"))?;
		Ok((value0, value1, value2))
	}
}

impl<'a, T1, T2, T3, T4> Deserialize<'a> for (T1, T2, T3, T4)
where
	T1: Deserialize<'a>,
	T2: Deserialize<'a>,
	T3: Deserialize<'a>,
	T4: Deserialize<'a>,
{
	fn deserialize(
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
		let value0 = <_>::deserialize(scope, value0)
			.map_err(|source| tg::error!(!source, "failed to deserialize the first value"))?;
		let value1 = <_>::deserialize(scope, value1)
			.map_err(|source| tg::error!(!source, "failed to deserialize the second value"))?;
		let value2 = <_>::deserialize(scope, value2)
			.map_err(|source| tg::error!(!source, "failed to deserialize the third value"))?;
		let value3 = <_>::deserialize(scope, value3)
			.map_err(|source| tg::error!(!source, "failed to deserialize the fourth value"))?;
		Ok((value0, value1, value2, value3))
	}
}

impl<'a, T> Deserialize<'a> for Vec<T>
where
	T: Deserialize<'a>,
{
	fn deserialize(
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
			let value = <_>::deserialize(scope, value)
				.map_err(|source| tg::error!(!source, "failed to deserialize the value"))?;
			output.push(value);
		}
		Ok(output)
	}
}

impl<'a, K, V> Deserialize<'a> for BTreeMap<K, V>
where
	K: Deserialize<'a> + Ord,
	V: Deserialize<'a>,
{
	fn deserialize(
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
			let key = <_>::deserialize(scope, key)
				.map_err(|source| tg::error!(!source, "failed to deserialize the key"))?;
			let value = <_>::deserialize(scope, value)
				.map_err(|source| tg::error!(!source, "failed to deserialize the value"))?;
			output.insert(key, value);
		}
		Ok(output)
	}
}

impl<'a> Deserialize<'a> for String {
	fn deserialize(
		scope: &mut v8::HandleScope<'a>,
		value: v8::Local<'a, v8::Value>,
	) -> tg::Result<Self> {
		if !value.is_string() {
			return Err(tg::error!("expected a string"));
		}
		Ok(value.to_rust_string_lossy(scope))
	}
}

impl<'a> Deserialize<'a> for PathBuf {
	fn deserialize(
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

impl<'a> Deserialize<'a> for Bytes {
	fn deserialize(
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
