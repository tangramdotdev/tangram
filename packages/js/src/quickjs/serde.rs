use {
	self::{deserializer::Deserializer, serializer::Serializer},
	rquickjs as qjs,
	std::ops::Deref,
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

impl<'js, T> qjs::IntoJs<'js> for Serde<T>
where
	T: serde::Serialize,
{
	fn into_js(self, ctx: &qjs::Ctx<'js>) -> qjs::Result<qjs::Value<'js>> {
		let serializer = Serializer::new(ctx.clone());
		let value = self
			.0
			.serialize(serializer)
			.map_err(|source| qjs::Error::Io(std::io::Error::other(source)))?;
		Ok(value)
	}
}

impl<'js, T> qjs::FromJs<'js> for Serde<T>
where
	T: serde::de::DeserializeOwned,
{
	fn from_js(ctx: &qjs::Ctx<'js>, value: qjs::Value<'js>) -> qjs::Result<Self> {
		let deserializer = Deserializer::new(ctx.clone(), value);
		let value = T::deserialize(deserializer)
			.map_err(|source| qjs::Error::Io(std::io::Error::other(source)))?;
		let value = Self(value);
		Ok(value)
	}
}
