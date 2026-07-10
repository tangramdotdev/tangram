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
	fn serialize<'js>(&self, ctx: &qjs::Ctx<'js>) -> tg::Result<qjs::Value<'js>> {
		let serializer = Serializer::new(ctx.clone());
		let value = self
			.0
			.serialize(serializer)
			.map_err(|error| tg::error!(!error, "failed to serialize the value to quickjs"))?;
		Ok(value)
	}
}

impl<'js, T> Deserialize<'js> for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn deserialize(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> tg::Result<Self> {
		let deserializer = Deserializer::new(ctx.clone(), value);
		let value = T::deserialize(deserializer)
			.map_err(|error| tg::error!(!error, "failed to deserialize the value from quickjs"))?;
		let value = Self(value);
		Ok(value)
	}
}

impl<'js, T> qjs::IntoJs<'js> for Serde<T>
where
	T: serde::Serialize,
{
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		self.serialize(ctx)
			.map_err(|error| qjs::Error::Io(std::io::Error::other(error)))
	}
}

impl<'js, T> qjs::FromJs<'js> for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn from_js(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<Self> {
		Self::deserialize(ctx, value).map_err(|error| qjs::Error::Io(std::io::Error::other(error)))
	}
}
