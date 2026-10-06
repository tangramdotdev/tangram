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

impl<'javascript, L, R> qjs::IntoJs<'javascript> for Either<L, R>
where
	L: qjs::IntoJs<'javascript>,
	R: qjs::IntoJs<'javascript>,
{
	fn into_js(self, ctx: &qjs::Ctx<'javascript>) -> qjs::Result<qjs::Value<'javascript>> {
		match self {
			Self::Left(left) => left.into_js(ctx),
			Self::Right(right) => right.into_js(ctx),
		}
	}
}

impl<'javascript, L, R> qjs::FromJs<'javascript> for Either<L, R>
where
	L: qjs::FromJs<'javascript>,
	R: qjs::FromJs<'javascript>,
{
	fn from_js(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>) -> qjs::Result<Self> {
		L::from_js(ctx, value.clone())
			.map(|left| Self::Left(left))
			.or_else(|_| R::from_js(ctx, value).map(|right| Self::Right(right)))
	}
}

impl<L, R> tangram_quickjs::Serialize for Either<L, R>
where
	L: tangram_quickjs::Serialize,
	R: tangram_quickjs::Serialize,
{
	fn serialize<'javascript>(
		&self,
		ctx: &qjs::Ctx<'javascript>,
	) -> tg::Result<qjs::Value<'javascript>> {
		match self {
			Self::Left(value) => tangram_quickjs::Serialize::serialize(value, ctx),
			Self::Right(value) => tangram_quickjs::Serialize::serialize(value, ctx),
		}
	}
}

impl<'javascript, L, R> tangram_quickjs::Deserialize<'javascript> for Either<L, R>
where
	L: tangram_quickjs::Deserialize<'javascript>,
	R: tangram_quickjs::Deserialize<'javascript>,
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

impl<'javascript> qjs::IntoJs<'javascript> for Uint8Array {
	fn into_js(self, ctx: &qjs::Ctx<'javascript>) -> qjs::Result<qjs::Value<'javascript>> {
		let typed_array = qjs::TypedArray::<u8>::new(ctx.clone(), self.0)?;
		let value = typed_array.into_value();
		Ok(value)
	}
}

impl<'javascript> qjs::FromJs<'javascript> for Uint8Array {
	fn from_js(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>) -> qjs::Result<Self> {
		let typed_array = qjs::TypedArray::<u8>::from_js(ctx, value.clone())?;
		// SAFETY: The bytes are copied without running JavaScript while the slice is alive.
		let bytes = unsafe { typed_array.as_bytes() }
			.ok_or_else(|| qjs::Error::new_from_js("Uint8Array", "bytes"))?
			.to_vec()
			.into();
		Ok(Self(bytes))
	}
}

impl tangram_quickjs::Serialize for Uint8Array {
	fn serialize<'javascript>(
		&self,
		ctx: &qjs::Ctx<'javascript>,
	) -> tg::Result<qjs::Value<'javascript>> {
		tangram_quickjs::Serialize::serialize(&self.0, ctx)
	}
}

impl<'javascript> tangram_quickjs::Deserialize<'javascript> for Uint8Array {
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		<Bytes as tangram_quickjs::Deserialize>::deserialize(ctx, value).map(Self)
	}
}
