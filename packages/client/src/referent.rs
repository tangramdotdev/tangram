use crate as tg;
use serde::ser::SerializeMap as _;
use std::{path::PathBuf, str::FromStr as _};
use tangram_uri as uri;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Referent<T> {
	pub item: T,
	pub path: Option<PathBuf>,
	pub tag: Option<tg::Tag>,
}

impl<T> Referent<T> {
	pub fn with_item(item: T) -> Self {
		Self {
			item,
			path: None,
			tag: None,
		}
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> tg::Referent<U> {
		tg::Referent {
			item: f(self.item),
			path: self.path,
			tag: self.tag,
		}
	}
}

impl<T> Referent<T>
where
	T: std::fmt::Display,
{
	pub fn to_uri(&self) -> uri::Reference {
		let mut builder = uri::Reference::builder().path(self.item.to_string());
		let mut query = Vec::new();
		if let Some(path) = &self.path {
			let path = path.to_string_lossy();
			let path = urlencoding::encode(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if let Some(tag) = &self.tag {
			let tag = tag.to_string();
			let tag = urlencoding::encode(&tag);
			let tag = format!("tag={tag}");
			query.push(tag);
		}
		if !query.is_empty() {
			builder = builder.query(query.join("&"));
		}
		builder.build().unwrap()
	}
}

impl<T> Referent<T>
where
	T: std::str::FromStr,
{
	pub fn with_uri(uri: &uri::Reference) -> tg::Result<Self> {
		let item = uri
			.path()
			.parse()
			.map_err(|_| tg::error!("failed to parse the item"))?;
		let mut path = None;
		let mut tag = None;
		if let Some(query) = uri.query() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=') {
					match key {
						"path" => {
							path = Some(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the path"))?
									.into_owned()
									.into(),
							);
						},
						"tag" => {
							tag = Some(
								urlencoding::decode(value)
									.map_err(|_| tg::error!("failed to decode the tag"))?
									.into_owned()
									.parse()
									.map_err(|_| tg::error!("failed to parse the tag"))?,
							);
						},
						_ => {},
					}
				}
			}
		}
		Ok(Self { item, path, tag })
	}
}

impl<T> std::fmt::Display for Referent<T>
where
	T: std::fmt::Display,
{
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl<T> std::str::FromStr for Referent<T>
where
	T: std::str::FromStr,
{
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri =
			uri::Reference::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}

impl<T> serde::Serialize for Referent<T>
where
	T: serde::Serialize,
{
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if let Ok(serde_json::Value::String(item)) = serde_json::to_value(&self.item) {
			let mut builder = uri::Reference::builder().path(item);
			let mut query = Vec::new();
			if let Some(path) = &self.path {
				let path = path.to_string_lossy();
				let path = urlencoding::encode(&path);
				let path = format!("path={path}");
				query.push(path);
			}
			if let Some(tag) = &self.tag {
				let tag = tag.to_string();
				let tag = urlencoding::encode(&tag);
				let tag = format!("tag={tag}");
				query.push(tag);
			}
			if !query.is_empty() {
				builder = builder.query(query.join("&"));
			}
			let uri = builder.build().unwrap();
			return uri.serialize(serializer);
		}
		let n =
			1 + if self.path.is_some() { 1 } else { 0 } + if self.tag.is_some() { 1 } else { 0 };
		let mut map = serializer.serialize_map(Some(n))?;
		map.serialize_entry("item", &self.item)?;
		if let Some(path) = &self.path {
			map.serialize_entry("path", path)?;
		}
		if let Some(tag) = &self.tag {
			map.serialize_entry("tag", tag)?;
		}
		map.end()
	}
}

impl<'de, T> serde::Deserialize<'de> for Referent<T>
where
	T: serde::Deserialize<'de>,
{
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct Visitor<T>(std::marker::PhantomData<T>);

		impl<'de, T> serde::de::Visitor<'de> for Visitor<T>
		where
			T: serde::Deserialize<'de>,
		{
			type Value = Referent<T>;

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("a string or map")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: serde::de::Error,
			{
				let uri = uri::Reference::from_str(value).map_err(serde::de::Error::custom)?;
				let item = T::deserialize(serde_json::Value::String(uri.path().to_owned()))
					.map_err(serde::de::Error::custom)?;
				let mut path = None;
				let mut tag = None;
				if let Some(query) = uri.query() {
					for param in query.split('&') {
						if let Some((key, value)) = param.split_once('=') {
							match key {
								"path" => {
									path = Some(
										urlencoding::decode(value)
											.map_err(serde::de::Error::custom)?
											.into_owned()
											.into(),
									);
								},
								"tag" => {
									tag = Some(
										urlencoding::decode(value)
											.map_err(serde::de::Error::custom)?
											.into_owned()
											.parse()
											.map_err(serde::de::Error::custom)?,
									);
								},
								_ => {},
							}
						}
					}
				}
				Ok(Referent { item, path, tag })
			}

			fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
			where
				A: serde::de::MapAccess<'de>,
			{
				let mut item = None;
				let mut path = None;
				let mut tag = None;
				while let Some(key) = map.next_key::<String>()? {
					match key.as_str() {
						"item" => {
							if item.is_some() {
								return Err(serde::de::Error::duplicate_field("item"));
							}
							item = Some(map.next_value()?);
						},
						"path" => {
							if path.is_some() {
								return Err(serde::de::Error::duplicate_field("path"));
							}
							path = Some(map.next_value()?);
						},
						"tag" => {
							if tag.is_some() {
								return Err(serde::de::Error::duplicate_field("tag"));
							}
							tag = Some(map.next_value()?);
						},
						_ => {
							let _ = map.next_value::<serde::de::IgnoredAny>()?;
						},
					}
				}
				let item = item.ok_or_else(|| serde::de::Error::missing_field("item"))?;
				Ok(Referent { item, path, tag })
			}
		}

		deserializer.deserialize_any(Visitor(std::marker::PhantomData))
	}
}
