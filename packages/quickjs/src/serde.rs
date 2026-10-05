use {
	self::{deserializer::Deserializer, serializer::Serializer},
	crate::{Deserialize, Serialize},
	rquickjs as qjs,
	std::ops::Deref,
	tangram_client::prelude::*,
};

mod deserializer;
mod serializer;

#[derive(Clone, Debug)]
pub struct Serde<T>(pub T);

impl<T> Deref for Serde<T> {
	type Target = T;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl<T> Serialize for Serde<T>
where
	T: serde::Serialize,
{
	fn serialize<'javascript>(
		&self,
		ctx: &qjs::Ctx<'javascript>,
	) -> tg::Result<qjs::Value<'javascript>> {
		let serializer = Serializer::new(ctx.clone());
		let value = self
			.0
			.serialize(serializer)
			.map_err(|error| tg::error!(!error, "failed to serialize the value to quickjs"))?;
		Ok(value)
	}
}

impl<'javascript, T> Deserialize<'javascript> for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(
		ctx: &qjs::Ctx<'javascript>,
		value: qjs::Value<'javascript>,
	) -> tg::Result<Self> {
		let deserializer = Deserializer::new(ctx.clone(), value);
		let value = T::deserialize(deserializer)
			.map_err(|error| tg::error!(!error, "failed to deserialize the value from quickjs"))?;
		let value = Self(value);
		Ok(value)
	}
}

impl<'javascript, T> qjs::IntoJs<'javascript> for Serde<T>
where
	T: serde::Serialize,
{
	fn into_js(self, ctx: &qjs::Ctx<'javascript>) -> qjs::Result<qjs::Value<'javascript>> {
		self.serialize(ctx)
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))
	}
}

impl<'javascript, T> qjs::FromJs<'javascript> for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn from_js(ctx: &qjs::Ctx<'javascript>, value: qjs::Value<'javascript>) -> qjs::Result<Self> {
		Self::deserialize(ctx, value).map_err(|error| qjs::Error::Io(std::io::Error::other(error)))
	}
}
