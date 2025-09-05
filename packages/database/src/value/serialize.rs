use crate::Value;
use bytes::Bytes;
use num::ToPrimitive as _;

pub trait Serialize {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>>;
}

impl<T> Serialize for &T
where
	T: Serialize,
{
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		(*self).serialize()
	}
}

impl Serialize for () {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		Ok(Value::Null)
	}
}

impl Serialize for bool {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		Ok(Value::Integer(if *self { 1 } else { 0 }))
	}
}

impl Serialize for u8 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for u16 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for u32 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for u64 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = self.to_i64().ok_or("value exceeds bounds")?;
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for usize {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = self.to_i64().ok_or("value exceeds bounds")?;
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for i8 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for i16 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for i32 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for i64 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = *self;
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for isize {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = self.to_i64().ok_or("value exceeds bounds")?;
		let value = Value::Integer(value);
		Ok(value)
	}
}

impl Serialize for f32 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = (*self).into();
		let value = Value::Real(value);
		Ok(value)
	}
}

impl Serialize for f64 {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = *self;
		let value = Value::Real(value);
		Ok(value)
	}
}

impl Serialize for String {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		Ok(Value::Text(self.clone()))
	}
}

impl Serialize for &str {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		Ok(Value::Text((*self).to_owned()))
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		match self {
			Some(value) => value.serialize(),
			None => Ok(Value::Null),
		}
	}
}

impl Serialize for Bytes {
	fn serialize(&self) -> Result<Value, Box<dyn std::error::Error + Send + Sync + 'static>> {
		let value = Value::Blob(self.clone());
		Ok(value)
	}
}
