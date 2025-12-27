use {bytes::Bytes, rquickjs as qjs, tangram_client::prelude::*};

#[derive(Clone, Debug)]
pub enum Either<L, R> {
	Left(L),
	Right(R),
}

impl<L, R> From<tg::Either<L, R>> for Either<L, R> {
	fn from(either: tg::Either<L, R>) -> Self {
		match either {
			tg::Either::Left(left) => Self::Left(left),
			tg::Either::Right(right) => Self::Right(right),
		}
	}
}

impl<L, R> From<Either<L, R>> for tg::Either<L, R> {
	fn from(value: Either<L, R>) -> Self {
		match value {
			Either::Left(left) => Self::Left(left),
			Either::Right(right) => Self::Right(right),
		}
	}
}

impl<'js, L, R> qjs::IntoJs<'js> for Either<L, R>
where
	L: qjs::IntoJs<'js>,
	R: qjs::IntoJs<'js>,
{
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		match self {
			Self::Left(left) => left.into_js(ctx),
			Self::Right(right) => right.into_js(ctx),
		}
	}
}

impl<'js, L, R> qjs::FromJs<'js> for Either<L, R>
where
	L: qjs::FromJs<'js>,
	R: qjs::FromJs<'js>,
{
	fn from_js(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<Self> {
		L::from_js(ctx, value.clone())
			.map(|left| Self::Left(left))
			.or_else(|_| R::from_js(ctx, value).map(|right| Self::Right(right)))
	}
}

#[derive(Clone, Debug)]
pub struct Uint8Array(pub Bytes);

impl From<Bytes> for Uint8Array {
	fn from(value: Bytes) -> Self {
		Self(value)
	}
}

impl From<Vec<u8>> for Uint8Array {
	fn from(value: Vec<u8>) -> Self {
		Self(value.into())
	}
}

impl From<Uint8Array> for Bytes {
	fn from(value: Uint8Array) -> Self {
		value.0
	}
}

impl<'js> qjs::IntoJs<'js> for Uint8Array {
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		let typed_array = qjs::TypedArray::<u8>::new(ctx.clone(), self.0)?;
		let value = typed_array.into_value();
		Ok(value)
	}
}

impl<'js> qjs::FromJs<'js> for Uint8Array {
	fn from_js(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<Self> {
		let typed_array = qjs::TypedArray::<u8>::from_js(ctx, value.clone())?;
		let slice = typed_array
			.as_bytes()
			.ok_or_else(|| qjs::Error::new_from_js("Uint8Array", "bytes"))?;
		let bytes = slice.to_vec().into();
		Ok(Self(bytes))
	}
}
