use {crate::Value, bytes::Bytes, num::ToPrimitive as _};

pub trait Deserialize: Sized {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

impl Deserialize for () {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		value.try_unwrap_null()?;
		Ok(())
	}
}

impl Deserialize for bool {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = value.try_unwrap_integer()?;
		let value = value > 0;
		Ok(value)
	}
}

macro_rules! integer {
	($t:ty) => {
		impl Deserialize for $t {
			fn deserialize(
				value: Value,
			) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
				let value = value.try_unwrap_integer()?;
				let value = value.try_into()?;
				Ok(value)
			}
		}
	};
}

integer!(u8);
integer!(u16);
integer!(u32);
integer!(u64);
integer!(usize);
integer!(i8);
integer!(i16);
integer!(i32);
integer!(i64);
integer!(isize);

impl Deserialize for f32 {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = value.try_unwrap_real()?;
		let value = value.to_f32().ok_or("invalid value")?;
		Ok(value)
	}
}

impl Deserialize for f64 {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = value.try_unwrap_real()?;
		Ok(value)
	}
}

impl Deserialize for String {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = value.try_unwrap_text()?;
		Ok(value)
	}
}

impl Deserialize for Bytes {
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let bytes = value.try_unwrap_blob()?;
		Ok(bytes)
	}
}

impl<T> Deserialize for Option<T>
where
	T: Deserialize,
{
	fn deserialize(
		value: Value,
	) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = match value {
			Value::Null => None,
			_ => Some(T::deserialize(value)?),
		};
		Ok(value)
	}
}

pub trait DeserializeAs<T> {
	fn deserialize_as(
		value: Value,
	) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

impl<T, U> DeserializeAs<Option<T>> for Option<U>
where
	U: DeserializeAs<T>,
{
	fn deserialize_as(
		value: Value,
	) -> Result<Option<T>, Box<dyn std::error::Error + Send + Sync + 'static>> {
		if matches!(value, Value::Null) {
			Ok(None)
		} else {
			U::deserialize_as(value).map(Some)
		}
	}
}

pub struct FromStr;

impl<T> DeserializeAs<T> for FromStr
where
	T: std::str::FromStr,
	T::Err: std::error::Error + Send + Sync + 'static,
{
	fn deserialize_as(
		value: Value,
	) -> Result<T, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let s = value.try_unwrap_text()?;
		s.parse().map_err(|e: T::Err| Box::new(e) as _)
	}
}
