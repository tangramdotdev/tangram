use {
	bytes::Bytes,
	rquickjs::{self as qjs, FromJs as _},
	std::{collections::BTreeMap, path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

pub trait Deserialize<'javascript>: Sized {
	fn deserialize(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>)
	-> tg::Result<Self>;
}

fn error(error: qjs::Error) -> tg::Error {
	tg::error!(
		source = std::io::Error::other(error),
		"failed to deserialize the value from quickjs"
	)
}

impl<'javascript> Deserialize<'javascript> for qjs::Value<'javascript> {
	fn deserialize(
		_ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		Ok(value)
	}
}

impl<'javascript> Deserialize<'javascript> for () {
	fn deserialize(
		_ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		if !value.is_null() && !value.is_undefined() {
			return Err(tg::error!("expected null or undefined"));
		}
		Ok(())
	}
}

macro_rules! impl_deserialize_with_from_js {
	($($type:ty),* $(,)?) => {
		$(
			impl<'javascript> Deserialize<'javascript> for $type {
				fn deserialize(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>) -> tg::Result<Self> {
					<$type>::from_js(ctx, value).map_err(error)
				}
			}
		)*
	};
}

impl_deserialize_with_from_js!(
	bool, u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64,
);

impl<'javascript, T> Deserialize<'javascript> for Box<T>
where
	T: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		Ok(Self::new(T::deserialize(ctx, value)?))
	}
}

impl<'javascript, T> Deserialize<'javascript> for Arc<T>
where
	T: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		Ok(Self::new(T::deserialize(ctx, value)?))
	}
}

impl<'javascript, L, R> Deserialize<'javascript> for tg::Either<L, R>
where
	L: Deserialize<'javascript>,
	R: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		L::deserialize(ctx, value.clone())
			.map(Self::Left)
			.or_else(|_| R::deserialize(ctx, value).map(Self::Right))
	}
}

impl<'javascript, T> Deserialize<'javascript> for Option<T>
where
	T: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		if value.is_null() || value.is_undefined() {
			Ok(None)
		} else {
			Ok(Some(T::deserialize(ctx, value)?))
		}
	}
}

macro_rules! impl_deserialize_tuple {
	($length:expr, $($index:tt => $type:ident),+ $(,)?) => {
		impl<'javascript, $($type),+> Deserialize<'javascript> for ($($type,)+)
		where
			$($type: Deserialize<'javascript>,)+
		{
			fn deserialize(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>) -> tg::Result<Self> {
				let array = qjs::Array::from_value(value).map_err(error)?;
				if array.len() != $length {
					return Err(tg::error!("invalid tuple length"));
				}
				Ok(($($type::deserialize(ctx, array.get::<qjs::Value>($index).map_err(error)?)?,)+))
			}
		}
	};
}

impl_deserialize_tuple!(1, 0 => T1);
impl_deserialize_tuple!(2, 0 => T1, 1 => T2);
impl_deserialize_tuple!(3, 0 => T1, 1 => T2, 2 => T3);
impl_deserialize_tuple!(4, 0 => T1, 1 => T2, 2 => T3, 3 => T4);

impl<'javascript, T> Deserialize<'javascript> for Vec<T>
where
	T: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		let array = qjs::Array::from_value(value).map_err(error)?;
		let mut output = Vec::with_capacity(array.len());
		for index in 0..array.len() {
			let value = array.get::<qjs::Value>(index).map_err(error)?;
			output.push(T::deserialize(ctx, value)?);
		}
		Ok(output)
	}
}

impl<'javascript, K, V> Deserialize<'javascript> for BTreeMap<K, V>
where
	K: Deserialize<'javascript> + Ord,
	V: Deserialize<'javascript>,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		let object = qjs::Object::from_value(value).map_err(error)?;
		let mut output = Self::new();
		for property in object.props::<String, qjs::Value>() {
			let (key, value) = property.map_err(error)?;
			let key = qjs::String::from_str(ctx.clone(), &key)
				.map(qjs::String::into_value)
				.map_err(error)?;
			output.insert(K::deserialize(ctx, key)?, V::deserialize(ctx, value)?);
		}
		Ok(output)
	}
}

impl<'javascript> Deserialize<'javascript> for String {
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		Self::from_js(ctx, value).map_err(error)
	}
}

impl<'javascript> Deserialize<'javascript> for PathBuf {
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		Ok(String::deserialize(ctx, value)?.into())
	}
}

impl<'javascript> Deserialize<'javascript> for Bytes {
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		let array = qjs::TypedArray::<u8>::from_js(ctx, value).map_err(error)?;
		// SAFETY: The bytes are copied without running JavaScript while the slice is alive.
		let bytes = unsafe { array.as_bytes() }
			.ok_or_else(|| tg::error!("expected a uint8array"))?
			.to_vec()
			.into();
		Ok(bytes)
	}
}
