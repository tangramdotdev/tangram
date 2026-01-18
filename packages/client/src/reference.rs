use {
	crate::prelude::*,
	std::{path::PathBuf, pin::pin},
	tangram_futures::stream::TryExt as _,
	tangram_uri::Uri,
};

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Reference {
	item: Item,
	options: Options,
	export: Option<String>,
}

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::From,
	derive_more::IsVariant,
	derive_more::TryInto,
	derive_more::TryUnwrap,
	derive_more::Unwrap,
)]
#[try_unwrap(ref)]
#[unwrap(ref)]
pub enum Item {
	Object(tg::object::Id),
	Path(PathBuf),
	Process(tg::process::Id),
	Tag(tg::tag::Pattern),
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub path: Option<PathBuf>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

impl Reference {
	#[must_use]
	pub fn new(item: Item, options: Options, export: Option<String>) -> Self {
		Self {
			item,
			options,
			export,
		}
	}

	#[must_use]
	pub fn with_item(item: Item) -> Self {
		Self {
			item,
			options: Options::default(),
			export: None,
		}
	}

	#[must_use]
	pub fn with_item_and_options(item: Item, options: Options) -> Self {
		Self {
			item,
			options,
			export: None,
		}
	}

	#[must_use]
	pub fn with_object(object: tg::object::Id) -> Self {
		Self::with_item(Item::Object(object))
	}

	#[must_use]
	pub fn with_path(path: PathBuf) -> Self {
		Self::with_item(Item::Path(path))
	}

	#[must_use]
	pub fn with_process(process: tg::process::Id) -> Self {
		Self::with_item(Item::Process(process))
	}

	#[must_use]
	pub fn with_tag(tag: tg::tag::Pattern) -> Self {
		Self::with_item(Item::Tag(tag))
	}

	#[must_use]
	pub fn item(&self) -> &Item {
		&self.item
	}

	#[must_use]
	pub fn options(&self) -> &Options {
		&self.options
	}

	#[must_use]
	pub fn export(&self) -> Option<&str> {
		self.export.as_deref()
	}

	pub fn with_uri(uri: &Uri) -> tg::Result<Self> {
		let path = uri.path();
		let item = path
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the reference item"))?;
		let mut options = Options::default();
		if let Some(query) = uri.query_raw() {
			for param in query.split('&') {
				if let Some((key, value)) = param.split_once('=') {
					let value = tangram_uri::decode_query_value(value)
						.map_err(|_| tg::error!("failed to decode the value"))?;
					match key {
						"local" => {
							options.local.replace(value.into_owned().into());
						},
						"path" => {
							options.path.replace(value.into_owned().into());
						},
						"remote" => {
							options.remote.replace(value.into_owned());
						},
						_ => {},
					}
				}
			}
		}
		let export = uri.fragment().map(ToOwned::to_owned);
		Ok(Self {
			item,
			options,
			export,
		})
	}

	#[must_use]
	pub fn to_uri(&self) -> Uri {
		let path = self.item.to_string();
		let mut builder = Uri::builder();
		builder = builder.path_raw(&path);
		let mut query = Vec::new();
		if let Some(local) = &self.options.local {
			let local = local.to_string_lossy();
			let local = tangram_uri::encode_query_value(&local);
			let local = format!("local={local}");
			query.push(local);
		}
		if let Some(path) = &self.options.path {
			let path = path.to_string_lossy();
			let path = tangram_uri::encode_query_value(&path);
			let path = format!("path={path}");
			query.push(path);
		}
		if let Some(remote) = &self.options.remote {
			let remote = tangram_uri::encode_query_value(remote);
			let remote = format!("remote={remote}");
			query.push(remote);
		}
		if !query.is_empty() {
			let query = query.join("&");
			builder = builder.query_raw(&query);
		}
		if let Some(export) = &self.export {
			builder = builder.fragment(export);
		}
		builder.build().unwrap()
	}

	pub async fn get<H>(
		&self,
		handle: &H,
	) -> tg::Result<tg::Referent<tg::Either<tg::Object, tg::Process>>>
	where
		H: tg::Handle,
	{
		let arg = tg::get::Arg::default();
		let stream = handle
			.get(self, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get stream"))?;
		let stream = pin!(stream);
		let output = stream
			.try_last()
			.await?
			.ok_or_else(|| tg::error!("expected an event"))?
			.try_unwrap_output()
			.ok()
			.ok_or_else(|| tg::error!("expected the output"))?;
		Ok(output)
	}

	#[must_use]
	pub fn is_solvable(&self) -> bool {
		self.item().is_tag()
	}
}

impl std::fmt::Display for Reference {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.to_uri())
	}
}

impl std::str::FromStr for Reference {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let uri = Uri::parse(value).map_err(|source| tg::error!(!source, "invalid uri"))?;
		let reference = Self::with_uri(&uri)?;
		Ok(reference)
	}
}

impl std::fmt::Display for Item {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Item::Object(object) => {
				write!(f, "{object}")?;
			},
			Item::Path(path) => {
				if path
					.components()
					.next()
					.is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
				{
					write!(f, "./")?;
				}
				write!(f, "{}", path.display())?;
			},
			Item::Process(process) => {
				write!(f, "{process}")?;
			},
			Item::Tag(tag) => {
				write!(f, "{tag}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if let Ok(object) = s.parse() {
			return Ok(Self::Object(object));
		}
		if s.starts_with('.') || s.starts_with('/') {
			let path = s.strip_prefix("./").unwrap_or(s).into();
			return Ok(Self::Path(path));
		}
		if let Ok(process) = s.parse() {
			return Ok(Self::Process(process));
		}
		if let Ok(tag) = s.parse() {
			return Ok(Self::Tag(tag));
		}
		Err(tg::error!(%s, "invalid item"))
	}
}

#[cfg(test)]
mod tests {
	use {crate::prelude::*, insta::assert_snapshot, std::path::PathBuf};

	#[test]
	fn test() {
		let id = "dir_010000000000000000000000000000000000000000000000000000"
			.parse()
			.unwrap();
		let reference = tg::Reference::with_object(id).to_string();
		assert_snapshot!(reference, @"dir_010000000000000000000000000000000000000000000000000000");

		let path = PathBuf::from("/foo/bar/../baz");
		let reference = tg::Reference::with_path(path).to_string();
		assert_snapshot!(reference, @"/foo/bar/../baz");

		let tag = "std/<0.0.1".parse().unwrap();
		let reference = tg::Reference::with_tag(tag).to_string();
		assert_snapshot!(reference, @"std/<0.0.1");
	}
}
