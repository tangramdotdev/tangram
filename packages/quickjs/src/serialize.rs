use {
	bytes::Bytes,
	rquickjs::{self as qjs, IntoJs as _},
	std::{collections::BTreeMap, path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

pub trait Serialize {
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>>;
}

fn error(error: qjs::Error) -> tg::Error {
	tg::error!(
		source = std::io::Error::other(error),
		"failed to serialize the value to quickjs"
	)
}

impl<T> Serialize for &T
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		(*self).serialize(ctx)
	}
}

impl Serialize for () {
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		Ok(qjs::Value::new_undefined(ctx.clone()))
	}
}

macro_rules! impl_serialize_with_into_js {
	($($type:ty),* $(,)?) => {
		$(
			impl Serialize for $type {
				fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
					(*self).into_js(ctx).map_err(error)
				}
			}
		)*
	};
}

impl_serialize_with_into_js!(
	bool, u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, f32, f64,
);

impl<T> Serialize for Box<T>
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		self.as_ref().serialize(ctx)
	}
}

impl<T> Serialize for Arc<T>
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		self.as_ref().serialize(ctx)
	}
}

impl<L, R> Serialize for tg::Either<L, R>
where
	L: Serialize,
	R: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		match self {
			Self::Left(value) => value.serialize(ctx),
			Self::Right(value) => value.serialize(ctx),
		}
	}
}

impl<T> Serialize for Option<T>
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		match self {
			Some(value) => value.serialize(ctx),
			None => Ok(qjs::Value::new_null(ctx.clone())),
		}
	}
}

macro_rules! impl_serialize_tuple {
	($($index:tt => $type:ident),+ $(,)?) => {
		impl<$($type),+> Serialize for ($($type,)+)
		where
			$($type: Serialize,)+
		{
			fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
				let array = qjs::Array::new(ctx.clone()).map_err(error)?;
				$(array.set($index, self.$index.serialize(ctx)?).map_err(error)?;)+
				Ok(array.into_value())
			}
		}
	};
}

impl_serialize_tuple!(0 => T1);
impl_serialize_tuple!(0 => T1, 1 => T2);
impl_serialize_tuple!(0 => T1, 1 => T2, 2 => T3);
impl_serialize_tuple!(0 => T1, 1 => T2, 2 => T3, 3 => T4);

impl<T> Serialize for [T]
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		let array = qjs::Array::new(ctx.clone()).map_err(error)?;
		for (index, value) in self.iter().enumerate() {
			array.set(index, value.serialize(ctx)?).map_err(error)?;
		}
		Ok(array.into_value())
	}
}

impl<T> Serialize for Vec<T>
where
	T: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		self.as_slice().serialize(ctx)
	}
}

impl<K, V> Serialize for BTreeMap<K, V>
where
	K: ToString,
	V: Serialize,
{
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		let object = qjs::Object::new(ctx.clone()).map_err(error)?;
		for (key, value) in self {
			object
				.set(key.to_string(), value.serialize(ctx)?)
				.map_err(error)?;
		}
		Ok(object.into_value())
	}
}

impl Serialize for String {
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		qjs::String::from_str(ctx.clone(), self)
			.map(qjs::String::into_value)
			.map_err(error)
	}
}

impl Serialize for PathBuf {
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		self.to_string_lossy().to_string().serialize(ctx)
	}
}

impl Serialize for Bytes {
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		qjs::TypedArray::<u8>::new(ctx.clone(), self.as_ref())
			.map(qjs::TypedArray::into_value)
			.map_err(error)
	}
}
